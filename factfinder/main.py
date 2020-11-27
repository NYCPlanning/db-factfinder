from census import Census
import json
import pandas as pd
import numpy as np
from pathlib import Path
from .variable import Variable
from .utils import get_c, get_p, get_z, outliers
from .multi import Pool
from .median import get_median, get_median_moe
from .special import special_variable_options
from .aggregate_geography import *
import logging
from functools import partial, lru_cache
import itertools
from cached_property import cached_property


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
        with open(f"{Path(__file__).parent}/data/special.json") as f:
            self.special = json.load(f)

        self.client_options = {
            "D": self.c.acs5dp,
            "S": self.c.acs5st,
            "P": self.c.sf1,
            "B": self.c.acs5,
        }

        self.aggregate_vertical_options = aggregate_vertical_options
        self.special_variable_options = special_variable_options
        self.outliers = outliers

    @cached_property
    def aggregated_geography(self) -> list:
        list3d = [
            [list(k.keys()) for k in i.values()]
            for i in self.aggregate_vertical_options.values()
        ]
        list2d = itertools.chain.from_iterable(list3d)
        return list(set(itertools.chain.from_iterable(list2d)))

    @cached_property
    def profile_only_variables(self) -> list: 
        return [
            i['pff_variable'] for i in self.metadata 
            if (i['census_variable'][0][0:2] == 'DP' and len(i['census_variable']) == 1)
        ]

    @cached_property
    def base_variables(self) -> list:
        """
        returns a list of base variables in the format of pff_variable
        """
        return list(set([i["base_variable"] for i in self.metadata]))

    @cached_property
    def median_variables(self) -> list:
        """
        returns a list of median variables in the format of pff_variable
        """
        return list(self.median.keys())

    @cached_property
    def special_variables(self) -> list:
        """
        returns a list of special calculation variables in the format 
        of pff_variable
        """
        return [i["pff_variable"] for i in self.special]

    def get_special_base_variables(self, pff_variable) -> list:
        """
        returns a list of special calculation base variables in the format 
        of pff_variable
        """
        special = list(
            filter(lambda x: x["pff_variable"] == pff_variable, self.special)
        )
        return special[0]["base_variables"]

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

    def calculate(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Main user interface, going through different following steps:
        1. calculate_c_e_m_p_z
        2. rounding
        3. assigning geoid
        """
        # 0. Initialize Variable class instance
        v = self.create_variable(pff_variable)
        # 1. get calculated values (c,e,m,p,z)
        df = self.calculate_c_e_m_p_z(v, geotype)
        # 2. rounding
        df = self.rounding(df, v.rounding)
        # 3. last round of data cleaning
        df = self.cleaning(df, v, geotype)
        return df

    def rounding(self, df: pd.DataFrame, digits: int) -> pd.DataFrame:
        """
        Round c, e, m, p, z fields based on rounding digits from metadata
        """
        df["c"] = df["c"].round(1)
        df["e"] = df["e"].round(digits)
        df["m"] = df["m"].round(digits)
        df["p"] = df["p"].round(1)
        df["z"] = df["z"].round(1)
        return df

    def cleaning(self, df: pd.DataFrame, v: Variable, geotype: str) -> pd.DataFrame:
        """
        last step data cleaning based on rules below:
        """
        # negative values are invalid
        df.loc[df.c < 0, "c"] = np.nan
        df.loc[df.e < 0, "e"] = np.nan
        df.loc[df.m < 0, "m"] = np.nan
        df.loc[df.p < 0, "p"] = np.nan
        df.loc[df.z < 0, "z"] = np.nan

        # p has to be less or equal to 100
        df.loc[df.p > 100, "p"] = np.nan

        # If p = np.nan/, then z = np.nan
        df.loc[(df.p.isna()) | (df.p == 100), "z"] = np.nan

        df.loc[df.e == 0, "c"] = np.nan
        df.loc[df.e == 0 & ~df.pff_variable.isin(self.base_variables), "m"] = np.nan
        df.loc[
            df.e == 0 & df.pff_variable.isin(self.base_variables) & df.m.isna(), "m"
        ] = 0
        df.loc[df.e == 0, "p"] = np.nan
        df.loc[df.e == 0, "z"] = np.nan

        df.loc[
            df.geotype.isin(["borough", "city"])
            & df.pff_variable.isin(self.base_variables)
            & df.c.isna(),
            "c",
        ] = 0

        df.loc[
            df.geotype.isin(["borough", "city"])
            & df.pff_variable.isin(self.base_variables)
            & df.m.isna(),
            "m",
        ] = 0

        df.loc[
            df.pff_variable.isin(self.base_variables),
            "p",
        ] = 100

        df.loc[
            df.pff_variable.isin(self.base_variables),
            "z",
        ] = np.nan

        return df

    def calculate_multiple_e_m(self, pff_variables: list, geotype: str) -> pd.DataFrame:
        """
        given a list of pff_variables, and geotype, calculate multiple 
        variables e, m at the same time using multiprocessing
        """
        _calculate_e_m = partial(self.calculate_e_m, geotype=geotype)
        with Pool(5) as download_pool:
            dfs = download_pool.map(_calculate_e_m, pff_variables)
        df = pd.concat(dfs)
        del dfs
        return df

    def calculate_special_e_m(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable.
        Used for variables requiring special horizontal aggregation techniques.
        """
        base_variables = self.get_special_base_variables(pff_variable)
        df = self.calculate_multiple_e_m(base_variables, geotype)
        df = self.special_variable_options[pff_variable](df, base_variables)
        df["pff_variable"] = pff_variable
        df["geotype"] = geotype
        return df[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def calculate_median_e_m(self, pff_variable, geotype) -> pd.DataFrame:
        """
        Given median variable in the form of pff_variable and geotype
        calculate the median and median moe
        """
        # 1. Initialize
        ranges = self.median_ranges(pff_variable)
        design_factor = self.median_design_factor(pff_variable)
        rounding = self.create_variable(pff_variable).rounding

        # 2. Calculate each variable that goes into median calculation
        df = self.calculate_multiple_e_m(list(ranges.keys()), geotype)

        # 3. create a pivot table with census_geoid as the index, and
        # pff_variable as column names. df_pivoted.e -> the estimation dataframe
        df_pivoted = df.loc[:, ["census_geoid", "pff_variable", "e"]].pivot(
            index="census_geoid", columns="pff_variable", values=["e"]
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
        e = df_pivoted.e.copy()
        e["e"] = results.loc[e.index == results.census_geoid, "e"].to_list()
        results["m"] = (
            e.loc[e.index == results.census_geoid, list(ranges.keys()) + ["e"]]
            .apply(lambda row: get_median_moe(ranges, row, design_factor), axis=1)
            .to_list()
        )

        # 6. return the output, containing the median, and all the variables used
        return results

    def create_variable(self, pff_variable: str) -> Variable:
        """
        given pff_variable name, return a Variable object
        """
        meta = list(filter(lambda x: x["pff_variable"] == pff_variable, self.metadata))[
            0
        ]
        return Variable(meta)

    @lru_cache(maxsize=128)
    def get_aggregate_vertical(self, source: str, geotype: str):
        """
        this function will aggregate over geographies, 
        e.g. aggregate over tracts to get NTA level data
        """
        source = "acs" if source != "decennial" else source
        to_geotype = geotype
        if geotype not in self.aggregated_geography:
            aggregate_vertical = lambda df: df
            from_geotype = geotype
        else:
            options = self.aggregate_vertical_options.get(source)
            for k, v in options.items():
                if geotype in v.keys():
                    from_geotype = k
                    aggregate_vertical = options[k][geotype]
        return from_geotype, aggregate_vertical

    @lru_cache(maxsize=128)
    def calculate_e_m(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable
        """
        # 1. create variable
        v = self.create_variable(pff_variable)

        # 2. pulling data from census site and aggregating
        from_geotype, aggregate_vertical = self.get_aggregate_vertical(
            v.source, geotype
        )
        df = self.aggregate_horizontal(v, from_geotype)
        df = aggregate_vertical(df)
        return df

    def calculate_e_m_p_z(self, pff_variable:str, geotype:str) -> pd.DataFrame:
        """
        This function is used for calculating profile variables only with 
        non-aggregated-geography geotype
        """
         # 1. create variable
        v = self.create_variable(pff_variable)
        # hard coding because by definition profile-only
        #  variables only has 1 census variable
        census_variable = v.census_variable[0]
        # 2. pulling data from census site and aggregating
        df = self.download_variable(self.download_e_m_p_z, v, geotype)
        df['pff_variable'] = pff_variable
        df['geotype'] = geotype
        # 3. Change field names
        columns = {
            census_variable+'E':'e',
            census_variable+'M':'m',
            census_variable+'PE':'p',
            census_variable+'PM':'z',
        }
        df = self.create_census_geoid(df, geotype)
        df = df.rename(columns=columns)
        return df[["census_geoid", "pff_variable", "geotype", "e", "m", "p", "z"]]

    def calculate_c_e_m_p_z(self, v: Variable, geotype: str) -> pd.DataFrame:
        """
        this function will calculate e, m first, then based on if the 
        variable is a median variable or base variable, it would calculate
        p and z accordingly. Note that c calculation is the same for all variables
        """
        output_fields = [
            "census_geoid",
            "pff_variable",
            "geotype",
            "c",
            "e",
            "m",
            "p",
            "z",
        ]
        # If pff_variable is a median variable, then we would need
        # to calculate using calculate_median_e_m for aggregated geography
        # there's no need to calculate p, z for median variables
        if (
            v.pff_variable in self.profile_only_variables 
            and geotype not in self.aggregated_geography
        ):
            df = self.calculate_e_m_p_z(v.pff_variable, geotype)

        elif v.pff_variable in self.median_variables:
            df = (
                self.calculate_median_e_m(v.pff_variable, geotype)
                if geotype in self.aggregated_geography
                else self.calculate_e_m(v.pff_variable, geotype)
            )
            df["p"] = 100 if geotype in ["city", "borough"] else np.nan
            df["z"] = np.nan
        else:
            df = (
                self.calculate_e_m(v.pff_variable, geotype)
                if not (
                    v.pff_variable in self.special_variables
                    and geotype in self.aggregated_geography
                )
                else self.calculate_special_e_m(v.pff_variable, geotype)
            )
            # If pff_variable is not base_variable, then p,z
            # are calculated against the base variable e(agg_e), m(agg_m)
            if v.pff_variable not in self.base_variables:
                df = self.calculate_e_m(v.pff_variable, geotype)
                if v.base_variable != 'nan':
                    df_base = (
                        self.calculate_e_m(v.base_variable, geotype)
                        if not (
                            v.base_variable in self.special_variables
                            and geotype in self.aggregated_geography
                        )
                        else self.calculate_special_e_m(v.base_variable, geotype)
                    )
                    df = df.merge(
                        df_base[["census_geoid", "e", "m"]].rename(
                            columns={"e": "agg_e", "m": "agg_m"}
                        ),
                        how="left",
                        on="census_geoid",
                    )
                    del df_base
                    df["p"] = df.apply(lambda row: get_p(row["e"], row["agg_e"]), axis=1)
                    df["z"] = df.apply(
                        lambda row: get_z(
                            row["e"], row["m"], row["p"], row["agg_e"], row["agg_m"]
                        ),
                        axis=1,
                    )
                else: 
                    # special case for grnorntpd, smpntc, 
                    # grpintc, nmsmpntc, cni1864_2, cvlf18t64
                    df["p"] = np.nan
                    df["z"] = np.nan
            # If pff_variable is a base variable, then
            # p = 100 for city and borough level, np.nan otherwise
            # z = np.nan for all levels of geography
            else:
                df["p"] = 100 if geotype in ["city", "borough"] else np.nan
                df["z"] = np.nan

        df["c"] = df.apply(lambda row: get_c(row["e"], row["m"]), axis=1)
        return df[output_fields]

    def create_census_variables(self, census_variable: list) -> (list, list):
        """
        Based on the census variables, spit out the 
        M variables and E variables
        e.g. ["B01001_044"] -> ["B01001_044M"], ["B01001_044E"]
        """
        E_variables = [i + "E" for i in census_variable if i[0] != "P"]
        if len(E_variables) == 0:  # Only decennial, pass raw variable name
            E_variables = census_variable
        M_variables = [i + "M" for i in census_variable if i[0] != "P"]
        return E_variables, M_variables

    def aggregate_horizontal(self, v: Variable, geotype: str) -> pd.DataFrame:
        """
        this function will aggregate multiple census_variables into 1 pff_variable
        e.g. ["B01001_044","B01001_020"] -> "mdpop65t66"
        """
        E_variables, M_variables = self.create_census_variables(v.census_variable)
        df = self.download_variable(self.download_e_m, v, geotype)

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
        df = self.create_census_geoid(df, geotype)

        # Output
        return df[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def create_census_geoid(self, df: pd.DataFrame, geotype: str) -> pd.DataFrame:
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
        return df

    def download_variable(self, download_function, v: Variable, geotype: str) -> pd.DataFrame:
        """
        Given a list of census_variables, and geotype, download data from acs/decennial api
        Note that depends on if we are taking PE/PM variables directly from census API,
        we will pass in self.download_e_m_p_z (data_profile_only) or self.download_e_m (generic)
        as download_func
        """
        geoqueries = self.get_geoquery(geotype)
        _download = partial(download_function, v=v)
        with Pool(5) as pool:
            dfs = pool.map(_download, geoqueries)
        df = pd.concat(dfs)
        return df

    def download_e_m_p_z(self, geoquery: dict, v: Variable) -> pd.DataFrame:
        """
        This function is for downloading non-aggregated-geotype and data profile only
        variables. It will return e, m, p, z variables for a single pff variable. 
        """
        # single source (data profile) only, so safe to set a default
        client = self.c.acs5dp
        census_variable = v.census_variable[0]
        E_variables = census_variable+'E'
        M_variables =  census_variable+'M'
        PE_variables =  census_variable+'PE'
        PM_variables =  census_variable+'PM'
        variables = [E_variables, M_variables, PE_variables, PM_variables]
        df = pd.DataFrame(client.get(
                ("NAME", ",".join(variables)),
                geoquery, year=self.year
            )
        )
        # If E is an outlier, then set M as Nan
        for var in variables: # Enforce type safety
            df[var] = df[var].astype('float64')
        df.loc[df[E_variables].isin(self.outliers), M_variables] = np.nan
        df.loc[df[E_variables]==0, M_variables]=0
        # Replace all outliers as Nan
        df = df.replace(self.outliers, np.nan)
        return df

    def download_e_m(self, geoquery: dict, v: Variable) -> pd.DataFrame:
        """
        this function works in conjunction with download_variable, 
        and is only created to facilitate multiprocessing, this function 
        if for generic variable calculation, returns e, m
        """
        # Get unique sources
        sources = set([i[0] for i in v.census_variable])
        frames = []
        for source in sources:
            # Create Variables for given source and set client
            variables = [i for i in v.census_variable if i[0] == source]
            client = self.client_options.get(source, self.c.acs5)
            E_variables, M_variables = self.create_census_variables(variables)
            frames.append(
                pd.DataFrame(
                    client.get(
                        ("NAME", ",".join(E_variables + M_variables)),
                        geoquery,
                        year=self.year,
                    )
                )
            )
        # Combine results from each source by joining on geo name
        df = frames[0]
        for i in frames[1:]:
            df = pd.merge(
                df,
                i[i.columns.difference(["state", "county", "tract", "place"])],
                left_on="NAME",
                right_on="NAME",
            )
        del frames
        # Enforce type safety
        for i in v.census_variable: 
            if i[0] != "P":
                df[f"{i}E"] = df[f"{i}E"].astype('float64')
                df[f"{i}M"] = df[f"{i}M"].astype('float64')
                # If E is zero, then set M as zero
                df.loc[df[f"{i}E"]==0, f"{i}M"] = 0
                # If E is an outlier, then set M as Nan
                df.loc[df[f"{i}E"].isin(self.outliers), f"{i}M"] = np.nan
            else:
                df[i] = df[i].astype('float64')
        # Replace all outliers as Nan
        df = df.replace(self.outliers, np.nan)
        return df

    def get_geoquery(self, geotype: str) -> list:
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
