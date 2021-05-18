import importlib
import os
from functools import partial
from multiprocessing import Pool

import numpy as np
import pandas as pd

from . import api_key
from .download import Download
from .metadata import Metadata, Variable


class Calculate:
    def __init__(self, year, source, geography):
        self.d = Download(
            api_key=api_key, year=year, source=source, geography=geography
        )
        self.meta = Metadata(year=year, source=source)
        AggregatedGeography = importlib.import_module(
            f"factfinder.geography.{self.geography}"
        ).AggregatedGeography
        self.geography = AggregatedGeography()

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
        del dfs
        return df

    def calculate_e_m(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable
        """
        cache_path = f".cache/{geotype}/{pff_variable}.pkl"
        if os.path.isfile(cache_path):
            df = pd.read_pickle(cache_path)
        else:
            # 1. create variable
            v = self.meta.create_variable(pff_variable)

            # 2. pulling data from census site and aggregating
            from_geotype, aggregate_vertical = self.get_aggregate_vertical(
                v.source, geotype
            )
            df = self.aggregate_horizontal(v, from_geotype)
            df = aggregate_vertical(df)
            os.makedirs(f".cache/{geotype}", exist_ok=True)
            self.write_to_cache(df, cache_path)
        return df

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
        df = self.d.download_variable(geotype, pff_variable)
        df["pff_variable"] = pff_variable
        df["geotype"] = geotype
        # 3. Change field names
        columns = {
            census_variable + "E": "e",
            census_variable + "M": "m",
            census_variable + "PE": "p",
            census_variable + "PM": "z",
        }
        df = self.create_census_geoid(df, geotype)
        df = df.rename(columns=columns)
        return df[["census_geoid", "pff_variable", "geotype", "e", "m", "p", "z"]]

    def calculate_poverty_p_z(self, v: Variable, geotype: str) -> pd.DataFrame:
        """
        For below poverty vars, the percent and percent MOE are taken from the ACS,
        but they come from E and M fields, not PE and PM fields. This function
        calculates the E and M for the associated percent variable, then renames as
        P and Z to join with the count variable.
        """
        pct_df = self.calculate_e_m(f"{v.pff_variable}_pct", geotype=geotype)
        pz = pct_df[["census_geoid", "geotype", "e", "m"]].rename(
            columns={"e": "p", "m": "z"}
        )
        return pz

    def aggregate_horizontal(self, v: Variable, geotype: str) -> pd.DataFrame:
        """
        this function will aggregate multiple census_variables into 1 pff_variable
        e.g. ["B01001_044","B01001_020"] -> "mdpop65t66"
        """
        E_variables, M_variables, _, _ = v.census_variables
        df = self.d.download_variable(v.pff_variable, geotype)

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
