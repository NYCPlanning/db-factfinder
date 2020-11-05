from census import Census
import json
import pandas as pd
from pathlib import Path
from .variable import Variable
from .utils import get_c, get_p, get_z
from .multi import Pool
from .median import get_median, get_median_moe
from .aggregate_geography import aggregate_nta
import logging
from functools import partial


class Pff:
    def __init__(self, api_key, year=2018):
        self.c = Census(api_key)
        self.year = year
        self.state = 36
        self.counties = ["005", "081", "085", "047", "061"]
        with open(f"{Path(__file__).parent}/data/metadata.json") as f:
            self.metadata = json.load(f)
        with open(f"{Path(__file__).parent}/data/median.json") as f:
            self.median = json.load(f)

    @property
    def base_variables(self) -> list:
        return list(set([i["base_variable"] for i in self.metadata]))

    @property
    def median_variables(self) -> list:
        return list(self.median.keys())

    def median_ranges(self, pff_variable) -> dict:
        return self.median[pff_variable]["ranges"]

    def median_design_factor(self, pff_variable) -> float:
        return self.median[pff_variable]["design_factor"]

    def calculate_median_variable(self, pff_variable, geotype):
        # Initialize
        ranges = self.median_ranges(pff_variable)
        design_factor = self.median_design_factor(pff_variable)
        rounding = self.create_variable(pff_variable).rounding

        # Calculate each variable that goes into median calculation
        _calculate_variable = partial(self.calculate_variable, geotype=geotype)
        with Pool(5) as download_pool:
            dfs = download_pool.map(_calculate_variable, list(ranges.keys()))
        df = pd.concat(dfs)
        del dfs

        # Iterate over unique_geoids and calculate median for each geoid
        df_pivoted = df.loc[:, ["acs_geoid", "pff_variable", "e", "m"]].pivot(
            index="acs_geoid", columns="pff_variable", values=["e", "m"]
        )

        results = pd.DataFrame()
        results["acs_geoid"] = df_pivoted.index
        results["pff_variable"] = pff_variable
        results["geotype"] = geotype
        results["e"] = (
            df_pivoted.e.loc[
                df_pivoted.e.index == results.acs_geoid, list(ranges.keys())
            ]
            .apply(lambda row: get_median(ranges, row), axis=1)
            .to_list()
        )
        m = df_pivoted.m
        m["e"] = results.loc[m.index == results.acs_geoid, "e"].to_list()
        results["m"] = (
            m.loc[m.index == results.acs_geoid, list(ranges.keys()) + ["e"]]
            .apply(lambda row: get_median_moe(ranges, row, design_factor), axis=1)
            .to_list()
        )

        output = pd.concat([df, results])
        return output

    def create_variable(self, pff_variable) -> Variable:
        meta = list(filter(lambda x: x["pff_variable"] == pff_variable, self.metadata))[
            0
        ]
        return Variable(meta)

    def calculate_variable(self, pff_variable, geotype) -> pd.DataFrame:
        # 1. create variable
        v = self.create_variable(pff_variable)

        # 2. identify source
        if v.source == "profile":
            source = self.c.acs5dp
        if v.source == "subject":
            source = self.c.acs5st
        source = self.c.acs5

        # 3. pulling data from census site
        if geotype == 'NTA': 
            df = self.aggregate_horizontal(source, v, "tract")
            df = self.aggregate_vertical(df, from_geotype='tract', to_geotype='NTA')
        else:
            df = self.aggregate_horizontal(source, v, "tract")
        return df

    def aggregate_horizontal(self, source, v, geotype) -> pd.DataFrame:
        """
        this function will aggregate multiple acs_variables into 1 pff_variable
        """
        # Create Variables
        E_variables = [i + "E" for i in v.acs_variable]
        M_variables = [i + "M" for i in v.acs_variable]
        acs_variables = E_variables + M_variables
        df = self.download_variable(source, acs_variables, geotype)

        # Aggregate variables horizontally
        df["pff_variable"] = v.pff_variable
        df["geotype"] = geotype
        df["e"] = df[E_variables].sum(axis=1)
        df["m"] = (df[M_variables] ** 2).sum(axis=1) ** 0.5

        # Create geoid
        if geotype == "tract":
            df["acs_geoid"] = df["state"] + df["county"] + df["tract"]
        elif geotype == "borough":
            df["acs_geoid"] = df["state"] + df["county"]
        elif geotype == "city":
            df["acs_geoid"] = df["state"] + df["place"]
        elif geotype == "block group":
            df["acs_geoid"] = (
                df["state"] + df["county"] + df["tract"] + df["block group"]
            )
        return df[["acs_geoid", "pff_variable", "geotype", "e", "m"]]

    def aggregate_vertical(self, df, from_geotype, to_geotype):
        """
        this function will aggregate over geographies, 
        e.g. aggregate over tracts to get NTA level data
        """
        if from_geotype == "tract" and to_geotype == "NTA":
            return aggregate_nta(df)

    def download_variable(self, source, variables, geotype) -> pd.DataFrame:
        geoqueries = self.get_geoquery(geotype)
        _download = partial(self.download, source=source, variables=variables)
        with Pool(5) as pool:
            dfs = pool.map(_download, geoqueries)
        df = pd.concat(dfs)
        if geotype in ["NTA"]:
            return self.aggregate_geography(df, from_geotype="tract", to_geotype="NTA")
        return df

    def download(self, geoquery, source, variables) -> pd.DataFrame:
        return pd.DataFrame(
            source.get(("NAME", ",".join(variables)), geoquery, year=self.year)
        )

    def get_geoquery(self, geotype) -> list:
        if geotype in ["tract", "NTA"]:
            return [
                {"for": "tract:*", "in": f"state:{self.state} county:{county}",}
                for county in self.counties
            ]
        elif geotype == "borough":
            return [
                {"for": f"county:{county}", "in": f"state:{self.state}",}
                for county in self.counties
            ]
        elif geotype == "city":
            return [{"for": "place:51000", "in": f"state:{self.state}",}]

        elif geotype in ["block group", "flood plain"]:
            return [
                {"for": "block group:*", "in": f"state:{self.state} county:{county}",}
                for county in self.counties
            ]
