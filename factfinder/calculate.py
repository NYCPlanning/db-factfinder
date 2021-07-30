import importlib
import os
from pathlib import Path

import numpy as np
import pandas as pd
from retry import retry

from .download import Download
from .median import Median
from .metadata import Metadata, Variable
from .special import *
from .utils import (
    get_c,
    get_p,
    get_z,
    rounding,
    write_to_cache,
)


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
        dfs = []
        for pff_variable in pff_variables:
            dfs.append(self.calculate_e_m(pff_variable, geotype))
        return pd.concat(dfs)

    def calculate_e_m(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable
        """
        cache_path = (
            ".cache/calculate"
            f"/year={self.year}"
            f"/geography={self.geography}"
            f"/geotype={geotype}"
            f"/{pff_variable}.pkl"
        )

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

        def get_median_and_median_moe(ranges, row, DF):
            md = Median(ranges, row, DF)
            try:
                e = md.median
            except:
                print("\n\n ===== Estimate error =====")
                print(f"ranges: {md.ranges}")
                print(f"B: {md.B}")
                print(f"cumm_dist: {md.cumm_dist}")
                return None
            try:
                m = md.median_moe
            except:
                print("\n\n ======= MOE error =======")
                print(f"Number of bins: {len(list(md.ranges))}")
                print(f"ranges: {md.ranges}")
                print(f"B: {md.B}")
                print(f"se_50: {md.se_50}")
                print(f"p_lower: {md.p_lower}")
                print(f"p_upper: {md.p_upper}")
                print(f"cumm_dist: {md.cumm_dist}")
                print(f"lower_bin: {md.lower_bin}")
                print(f"upper_bin: {md.upper_bin}")
                return None
            return pd.Series({'e': e, 'm': m})

        results = df_pivoted.e.apply(lambda x: get_median_and_median_moe(
            ranges, x, DF=design_factor), axis=1)
        results["census_geoid"] = df_pivoted.index
        results = results.reset_index(drop=True)
        results["pff_variable"] = pff_variable
        results["geotype"] = geotype
        return results[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def calculate_poverty_p_z(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        For below poverty vars, the percent and percent MOE are taken from the ACS,
        but they come from E and M fields, not PE and PM fields. This function
        calculates the E and M for the associated percent variable, then renames as
        P and Z to join with the count variable.
        """
        pct_df = self.calculate_e_m(f"{pff_variable}_pct", geotype=geotype)
        pz = pct_df[["census_geoid", "geotype", "e", "m"]].rename(
            columns={"e": "p", "m": "z"}
        )
        return pz

    def calculate_e_m_special(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        Given pff_variable and geotype, download and calculate the variable.
        Used for variables requiring special horizontal aggregation techniques.
        """
        assert pff_variable in self.meta.special_variables
        base_variables = self.meta.get_special_base_variables(pff_variable)
        df = self.calculate_e_m_multiprocessing(base_variables, geotype)
        func = globals()[pff_variable]
        df = func(df, base_variables)
        df["pff_variable"] = pff_variable
        df["geotype"] = geotype
        return df[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def calculate_c_e_m_p_z(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        """
        this function will calculate e, m first, then based on if the
        variable is a median variable or base variable, it would calculate
        p and z accordingly. Note that c calculation is the same for all variables
        """
        # If pff_variable is a median variable, then we would need
        # to calculate using calculate_median_e_m for aggregated geography
        # there's no need to calculate p, z for median variables
        v = self.meta.create_variable(pff_variable)
        if (
            pff_variable in self.meta.profile_only_variables
            and geotype not in self.geo.aggregated_geography
        ):
            df = self.calculate_e_m_p_z(pff_variable, geotype)

        elif pff_variable in self.meta.median_variables:
            df = (
                self.calculate_e_m_median(pff_variable, geotype)
                if geotype in self.geo.aggregated_geography
                else self.calculate_e_m(pff_variable, geotype)
            )
            df["p"] = 100 if geotype in ["city", "borough"] else np.nan
            df["z"] = np.nan
        else:
            df = (
                self.calculate_e_m(pff_variable, geotype)
                if not (
                    (
                        pff_variable in self.meta.special_variables
                        and geotype in self.geo.aggregated_geography
                    )
                    or (pff_variable == "wrkrnothm")
                )
                # We only calculate special variables for aggregated geographies,
                # with the exception of 'wrkrnothm' (calculate for both aggregated and non-aggregated geographies)
                else self.calculate_e_m_special(pff_variable, geotype)
            )
            # If pff_variable is not base_variable, then p,z
            # are calculated against the base variable e(agg_e), m(agg_m)
            if pff_variable not in self.meta.base_variables:
                if (
                    pff_variable in ["pbwpv", "pu18bwpv", "p65plbwpv"]
                    and geotype not in self.geo.aggregated_geography
                    and self.year != 2010
                ):
                    # special case for poverty variables
                    df_pz = self.calculate_poverty_p_z(pff_variable, geotype)
                    df = df.merge(df_pz, on=["census_geoid", "geotype"])
                elif v.base_variable != "nan":
                    if (
                        v.base_variable in self.meta.special_variables
                        and geotype in self.geo.aggregated_geography
                    ):
                        df_base = self.calculate_e_m_special(
                            v.base_variable, geotype)
                    if (
                        v.base_variable in self.meta.median_variables
                        and geotype in self.geo.aggregated_geography
                    ):
                        df_base = self.calculate_e_m_median(
                            v.base_variable, geotype)
                    else:
                        df_base = self.calculate_e_m(v.base_variable, geotype)

                    df = df.merge(
                        df_base[["census_geoid", "e", "m"]].rename(
                            columns={"e": "agg_e", "m": "agg_m"}
                        ),
                        how="left",
                        on="census_geoid",
                    )
                    del df_base
                    df["p"] = df.apply(
                        lambda row: get_p(row["e"], row["agg_e"]), axis=1
                    )
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
        return df[["census_geoid", "pff_variable", "geotype", "c", "e", "m", "p", "z"]]

    def cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
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
        df.loc[
            df.e == 0 & ~df.pff_variable.isin(self.meta.base_variables), "m"
        ] = np.nan
        df.loc[
            df.e == 0 & df.pff_variable.isin(
                self.meta.base_variables) & df.m.isna(),
            "m",
        ] = 0
        df.loc[df.e == 0, "p"] = np.nan
        df.loc[df.e == 0, "z"] = np.nan

        df.loc[
            df.geotype.isin(["borough", "city"])
            & df.pff_variable.isin(self.meta.base_variables)
            & df.c.isna(),
            "c",
        ] = 0

        df.loc[
            df.geotype.isin(["borough", "city"])
            & df.pff_variable.isin(self.meta.base_variables)
            & df.m.isna(),
            "m",
        ] = 0

        df.loc[
            df.pff_variable.isin(self.meta.base_variables)
            & ~df.pff_variable.isin(self.meta.median_variables),
            "p",
        ] = 100

        df.loc[
            df.pff_variable.isin(self.meta.base_variables)
            & ~df.pff_variable.isin(self.meta.median_variables),
            "z",
        ] = np.nan

        df.loc[
            df.pff_variable.isin(self.meta.median_inputs)
            & ~df.pff_variable.str.contains("rms"),
            "m",
        ] = np.nan
        df.loc[
            df.pff_variable.isin(self.meta.median_inputs)
            & ~df.pff_variable.str.contains("rms"),
            "p",
        ] = np.nan
        df.loc[
            df.pff_variable.isin(self.meta.median_inputs)
            & ~df.pff_variable.str.contains("rms"),
            "z",
        ] = np.nan
        df.loc[
            df.pff_variable.isin(self.meta.median_inputs)
            & ~df.pff_variable.str.contains("rms"),
            "c",
        ] = np.nan

        return df

    def labs_geoid(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Format geoid and geotype to match Planning Labs standards
        """
        df["labs_geoid"] = df.census_geoid.apply(self.geo.format_geoid)
        df["labs_geotype"] = df.geotype.apply(
            lambda x: self.geo.format_geotype(x))

        return df[
            [
                "census_geoid",
                "labs_geoid",
                "geotype",
                "labs_geotype",
                "pff_variable",
                "c",
                "e",
                "m",
                "p",
                "z",
            ]
        ]

    @retry(tries=3, delay=30)
    def __call__(self, pff_variable: str, geotype: str) -> pd.DataFrame:
        # 0. Initialize Variable class instance
        v = self.meta.create_variable(pff_variable)
        # 1. get calculated values (c,e,m,p,z)
        df = self.calculate_c_e_m_p_z(pff_variable, geotype)
        # 2. rounding
        df = rounding(df, v.rounding)
        # 3. last round of data cleaning
        df = self.cleaning(df)
        # 4. Assign Labs geoid and geotype
        df = self.labs_geoid(df)
        return df
