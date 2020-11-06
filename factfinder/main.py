from census import Census
import json
import pandas as pd
from pathlib import Path
from .variable import Variable
from .utils import get_c, get_p, get_z
from .multi import Pool
from .median import get_median, get_median_moe
from .aggregate_geography import *
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
        """
        returns a list of base variables in the format of pff_variable
        """
        return list(set([i["base_variable"] for i in self.metadata]))

    @property
    def median_variables(self) -> list:
        """
        returns a list of median variables in the format of pff_variable
        """
        return list(self.median.keys())

    def median_ranges(self, pff_variable) -> dict:
        """
        given median variable in the format of pff_variable
        returns the ranges object for the median variable. 
        e.g. 
        {
            'mdpop0t4': [0, 4.9999],
            'mdpop5t9': [5, 9.9999],
            ...
        }
        """
        return self.median[pff_variable]["ranges"]

    def median_design_factor(self, pff_variable) -> float:
        """
        given median variable in the form of pff_variable
        returns the design_factor needed to calculate the 
        median moe
        """
        return self.median[pff_variable]["design_factor"]

    def calculate_median_variable(self, pff_variable, geotype):
        """
        Given median variable in the form of pff_variable and geotype
        calculate the median and median moe
        """
        # 1. Initialize
        ranges = self.median_ranges(pff_variable)
        design_factor = self.median_design_factor(pff_variable)
        rounding = self.create_variable(pff_variable).rounding

        # 2. Calculate each variable that goes into median calculation
        _calculate_variable = partial(self.calculate_variable, geotype=geotype)
        with Pool(5) as download_pool:
            dfs = download_pool.map(_calculate_variable, list(ranges.keys()))
        df = pd.concat(dfs)
        del dfs

        # 3. create a pivot table with census_geoid as the index, and pff_variable as column names.
        # df_pivoted.e -> the estimation dataframe
        # df_pivoted.m -> the moe dataframe
        df_pivoted = df.loc[:, ["census_geoid", "pff_variable", "e", "m"]].pivot(
            index="census_geoid", columns="pff_variable", values=["e", "m"]
        )

        # Empty dataframe to store the results
        results = pd.DataFrame()
        results["census_geoid"] = df_pivoted.index
        results["pff_variable"] = pff_variable
        results["geotype"] = geotype

        # 4. calculate median estimation using get_median
        results["e"] = (
            df_pivoted.e.loc[
                df_pivoted.e.index == results.census_geoid, list(ranges.keys())
            ]
            .apply(lambda row: get_median(ranges, row), axis=1)
            .to_list()
        )

        # 5. Calculate median moe using get_median_moe
        # Note that median moe calculation needs the median estimation
        # so we seperated df_pivoted.m out as a seperate dataframe
        m = df_pivoted.m
        m["e"] = results.loc[m.index == results.census_geoid, "e"].to_list()
        results["m"] = (
            m.loc[m.index == results.census_geoid, list(ranges.keys()) + ["e"]]
            .apply(lambda row: get_median_moe(ranges, row, design_factor), axis=1)
            .to_list()
        )

        # 6. return the output, containing the median, and all the variables used
        output = pd.concat([df, results])
        return output

    def create_variable(self, pff_variable) -> Variable:
        """
        given pff_variable name, return a Variable object
        """
        meta = list(filter(lambda x: x["pff_variable"] == pff_variable, self.metadata))[
            0
        ]
        return Variable(meta)

    def calculate_variable(self, pff_variable, geotype) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable
        """
        # 1. create variable
        v = self.create_variable(pff_variable)

        # 2. identify source
        if v.source == "profile":
            client = self.c.acs5dp
        elif v.source == "subject":
            client = self.c.acs5st
        elif v.source == "decennial":
            client = self.c.sf1
        else:
            client = self.c.acs5

        # 3. pulling data from census site
        if geotype == "NTA":
            df = self.aggregate_horizontal(client, v, "tract")
            df = self.aggregate_vertical(
                df, from_geotype="tract", to_geotype="NTA"
                )
        elif geotype == "cd_fp_500":
            if v.source == "decennial":
                df = self.aggregate_horizontal(client, v, "block")
                df = self.aggregate_vertical(
                    df, from_geotype="block", to_geotype="cd_fp_500"
                )
            else:
                df = self.aggregate_horizontal(client, v, "block group")
                df = self.aggregate_vertical(
                    df, from_geotype="block group", to_geotype="cd_fp_500"
                )
        elif geotype == "cd_fp_100":
            if v.source == "decennial":
                df = self.aggregate_horizontal(client, v, "block")
                df = self.aggregate_vertical(
                    df, from_geotype="block", to_geotype="cd_fp_100"
                )
            else:
                df = self.aggregate_horizontal(client, v, "block group")
                df = self.aggregate_vertical(
                    df, from_geotype="block group", to_geotype="cd_fp_100"
                )
        elif geotype == "cd_park_access":
            if v.source == "decennial":
                df = self.aggregate_horizontal(client, v, "block")
                df = self.aggregate_vertical(
                    df, from_geotype="block", to_geotype="cd_park_access"
                )
            else:
                df = self.aggregate_horizontal(client, v, "block group")
                df = self.aggregate_vertical(
                    df, from_geotype="block group", to_geotype="cd_park_access"
                )
        else:
            # If not spatial aggregation needed, just aggregate_horizontal
            df = self.aggregate_horizontal(client, v, geotype)
        return df

    def aggregate_horizontal(self, client, v, geotype) -> pd.DataFrame:
        """
        this function will aggregate multiple census_variables into 1 pff_variable
        e.g. ["B01001_044","B01001_020"] -> "mdpop65t66"
        """
        # Create Variables
        E_variables = (
            [i + "E" for i in v.census_variable]
            if v.source != "decennial"
            else v.census_variable
        )
        M_variables = (
            [i + "M" for i in v.census_variable] 
            if v.source != "decennial" 
            else []
        )
        census_variables = E_variables + M_variables
        df = self.download_variable(client, census_variables, geotype)

        # Aggregate variables horizontally
        df["pff_variable"] = v.pff_variable
        df["geotype"] = geotype
        df["e"] = df[E_variables].sum(axis=1)
        df["m"] = (
            (df[M_variables] ** 2).sum(axis=1) ** 0.5
            if v.source != "decennial"
            else np.nan
        )

        # Create geoid
        if geotype == "tract":
            df["census_geoid"] = df["state"] + df["county"] + df["tract"]
        elif geotype == "borough":
            df["census_geoid"] = df["state"] + df["county"]
        elif geotype == "city":
            df["census_geoid"] = df["state"] + df["place"]
        elif geotype == "block":
            df["census_geoid"] = df["state"] + df["county"] + df["tract"] + df["block"]
        elif geotype == "block group":
            df["census_geoid"] = (
                df["state"] + df["county"] + df["tract"] + df["block group"]
            )
        return df[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def aggregate_vertical(self, df, from_geotype, to_geotype):
        """
        this function will aggregate over geographies, 
        e.g. aggregate over tracts to get NTA level data
        """
        if from_geotype == "tract" and to_geotype == "NTA":
            return tract_to_nta(df)
        elif from_geotype == "tract" and to_geotype == "cd":
            return tract_to_cd(df)
        elif from_geotype == "block group" and to_geotype == "cd_fp_500":
            return block_group_to_cd_fp500(df)
        elif from_geotype == "block group" and to_geotype == "cd_fp_100":
            return block_group_to_cd_fp100(df)
        elif from_geotype == "block group" and to_geotype == "cd_park_access":
            return block_group_to_cd_park_access(df)
        elif from_geotype == "block" and to_geotype == "cd_fp_500":
            return block_to_cd_fp500(df)
        elif from_geotype == "block" and to_geotype == "cd_fp_100":
            return block_to_cd_fp100(df)
        elif from_geotype == "block" and to_geotype == "cd_park_access":
            return block_to_cd_park_access(df)

    def download_variable(self, client, variables, geotype) -> pd.DataFrame:
        """
        Given a list of census_variables, and geotype, download data from acs api
        """
        geoqueries = self.get_geoquery(geotype)
        _download = partial(self.download, client=client, variables=variables)
        with Pool(5) as pool:
            dfs = pool.map(_download, geoqueries)
        df = pd.concat(dfs)
        return df

    def download(self, geoquery, client, variables) -> pd.DataFrame:
        """
        this function works in conjunction with download_variable, 
        and is only created to facilitate multiprocessing
        """
        return pd.DataFrame(
            client.get(("NAME", ",".join(variables)), geoquery, year=self.year)
        )

    def get_geoquery(self, geotype) -> list:
        """
        given geotype, this function will create a list of geographic queries
        we would need to pull NYC level data. 
        """
        if geotype == "tract":
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

        elif geotype == "block":
            return [
                {"for": "block:*", "in": f"state:{self.state} county:{county}",}
                for county in self.counties
            ]

        elif geotype in "block group":
            return [
                {"for": "block group:*", "in": f"state:{self.state} county:{county}",}
                for county in self.counties
            ]
