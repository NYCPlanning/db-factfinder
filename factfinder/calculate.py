import importlib
import os
from functools import partial
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

from .download import Download
from .metadata import Metadata, Variable
from .utils import get_median, get_median_moe


def write_to_cache(df: pd.DataFrame, path: str):
    """
    this function will cache a dataframe to a given path
    """
    if not os.path.isfile(path):
        df.to_pickle(path)
    return None


class Calculate:
    def __init__(self, api_key, year, source, geography):
        self.year = year
        self.source = source
        self.geography = geography
        self.d = Download(
            api_key=api_key, year=year, source=source, geography=geography
        )
        self.meta = Metadata(year=year, source=source)
        AggregatedGeography = importlib.import_module(
            f"factfinder.geography.{geography}"
        ).AggregatedGeography
        self.geo = AggregatedGeography()

    def calculate_e_m_multiprocessing(
        self, pff_variables: list, geotype: str
    ) -> pd.DataFrame:
        """
        given a list of pff_variables, and geotype, calculate multiple
        variables e, m at the same time using multiprocessing
        """
        _calculate_e_m = partial(self.calculate_e_m, geotype=geotype)
        with Pool(5) as download_pool:
            dfs = download_pool.map(_calculate_e_m, pff_variables)
        df = pd.concat(dfs)
        return df

    def calculate_e_m(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable
        """
        cache_path = f".cache/year={self.year}/geography={self.geography}\
            /geotype={geotype}/{pff_variable}.pkl"

        if os.path.isfile(cache_path):
            df = pd.read_pickle(cache_path)
        else:
            # 0. create variable
            v = self.meta.create_variable(pff_variable)

            # 1. Determin from and to geotype
            to_geotype = geotype
            if geotype not in self.geo.aggregated_geography:
                from_geotype = geotype

                def aggregate_vertical(df):
                    return df

            else:
                options = self.geo.options.get(self.source)
                for k, val in options.items():
                    if geotype in val.keys():
                        from_geotype = k
                aggregate_vertical = options[from_geotype][to_geotype]

            # 2. Download Dataframe for given geotype
            df = self.d(from_geotype, pff_variable)

            # 3. Aggregate by variable (horizontal) first
            df = self.aggregate_horizontal(df, v)

            # 4. Aggregate by Geography (vertical) first
            df = aggregate_vertical(df)

            # 5. Caching
            os.makedirs(Path(cache_path).parent, exist_ok=True)
            write_to_cache(df, cache_path)
        return df

    def aggregate_horizontal(self, df: pd.DataFrame, v: Variable) -> pd.DataFrame:
        """
        this function will aggregate multiple census_variables into 1 pff_variable
        e.g. ["B01001_044","B01001_020"] -> "mdpop65t66"
        """
        E_variables, M_variables, _, _ = v.census_variables

        # Aggregate variables horizontally
        df["e"] = df[E_variables].sum(axis=1)
        df["m"] = (
            (df[M_variables] ** 2).sum(axis=1) ** 0.5
            if v.source != "decennial"
            else np.nan
        )
        # Output
        return df[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def calculate_e_m_p_z(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        This function is used for calculating profile variables only with
        non-aggregated-geography geotype
        """
        # 1. create variable
        v = self.meta.create_variable(pff_variable)
        # hard coding because by definition profile-only
        #  variables only has 1 census variable
        census_variable = v.census_variable[0]
        # 2. pulling data from census site and aggregating
        df = self.d(geotype, pff_variable)
        # 3. Change field names
        columns = {
            census_variable + "E": "e",
            census_variable + "M": "m",
            census_variable + "PE": "p",
            census_variable + "PM": "z",
        }
        df = df.rename(columns=columns)
        return df[["census_geoid", "pff_variable", "geotype", "e", "m", "p", "z"]]

    def calculate_e_m_median(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given median variable in the form of pff_variable and geotype
        calculate the median and median moe
        """
        # 1. Initialize
        ranges = self.meta.median_ranges(pff_variable)
        design_factor = self.meta.median_design_factor(pff_variable)

        # 2. Calculate each variable that goes into median calculation
        df = self.calculate_e_m_multiprocessing(list(ranges.keys()), geotype)

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

        # def calculate_poverty_p_z(self, v: Variable, geotype: str) -> pd.DataFrame:
        #     """
        #     For below poverty vars, the percent and percent MOE are taken from the ACS,
        #     but they come from E and M fields, not PE and PM fields. This function
        #     calculates the E and M for the associated percent variable, then renames as
        #     P and Z to join with the count variable.
        #     """
        #     pct_df = self.calculate_e_m(f"{v.pff_variable}_pct", geotype=geotype)
        #     pz = pct_df[["census_geoid", "geotype", "e", "m"]].rename(
        #         columns={"e": "p", "m": "z"}
        #     )
        #     return pz

    # def __call__(self, geotype: str, pff_variable: str) -> pd.DataFrame:
