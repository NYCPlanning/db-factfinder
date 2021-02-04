import math
from pathlib import Path

import numpy as np
import pandas as pd
from cached_property import cached_property


class AggregatedGeography:
    def __init__(self, year=2018):
        self.year = year

    @cached_property
    def lookup_geo(self):
        year = (
            self.year // 10 * 10
        )  # find the current decennial year based on given year
        lookup_geo = pd.read_csv(
            f"{Path(__file__).parent}/data/lookup_geo/{year}/lookup_geo.csv",
            dtype="str",
        )
        lookup_geo["geoid_block"] = lookup_geo.county_fips + lookup_geo.ctcb2010
        lookup_geo["geoid_block_group"] = lookup_geo.geoid_block.apply(
            lambda x: x[0:12]
        )
        lookup_geo["geoid_tract"] = lookup_geo.county_fips + lookup_geo.ct2010
        lookup_geo["cd_fp_500"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["fp_500"]) else np.nan, axis=1
        )
        lookup_geo["cd_fp_100"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["fp_100"]) else np.nan, axis=1
        )
        lookup_geo["cd_park_access"] = lookup_geo.apply(
            lambda row: row["cd"] if int(row["park_access"]) else np.nan, axis=1
        )
        return lookup_geo

    @staticmethod
    def agg_moe(x):
        return math.sqrt(sum([i ** 2 for i in x]))

    @staticmethod
    def create_output(df, colname):
        """
        this function will calculate the aggregated e and m
        given colname we would like to aggregate over
        """
        return (
            df[[colname, "e"]]
            .groupby([colname])
            .sum()
            .merge(
                df[[colname, "m"]].groupby([colname]).agg(AggregatedGeography.agg_moe),
                on=colname,
            )
            .reset_index()
            .rename(columns={colname: "census_geoid"})
        )

    def tract_to_nta(self, df):
        df = df.merge(
            self.lookup_geo[["geoid_tract", "nta"]].drop_duplicates(),
            how="left",
            right_on="geoid_tract",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "nta")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "NTA"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_fp500(self, df):
        """
        500 yr flood plain aggregation for block group data (ACS)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_500.isna(), ["geoid_block_group", "cd_fp_500"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_500")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_500"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_fp100(self, df):
        """
        100 yr flood plain aggregation for block group data (ACS)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_100.isna(), ["geoid_block_group", "cd_fp_100"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_100")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_100"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_group_to_cd_park_access(self, df):
        """
        walk-to-park access zone aggregation for block group data (acs)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_park_access.isna(),
                ["geoid_block_group", "cd_park_access"],
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block_group",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_park_access")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_park_access"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_fp500(self, df):
        """
        500 yr flood plain aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_500.isna(), ["geoid_block", "cd_fp_500"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_500")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_500"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_fp100(self, df):
        """
        100 yr flood plain aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_fp_100.isna(), ["geoid_block", "cd_fp_100"]
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_fp_100")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_fp_100"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def block_to_cd_park_access(self, df):
        """
        walk-to-park access zone aggregation for block data (decennial)
        """
        df = df.merge(
            self.lookup_geo.loc[
                ~self.lookup_geo.cd_park_access.isna(),
                ["geoid_block", "cd_park_access"],
            ].drop_duplicates(),
            how="right",
            right_on="geoid_block",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd_park_access")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd_park_access"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    def tract_to_cd(self, df):
        """
        tract to cd
        """
        df = df.merge(
            self.lookup_geo[["geoid_tract", "cd"]].drop_duplicates(),
            how="left",
            right_on="geoid_tract",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cd")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "cd"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    @cached_property
    def aggregate_vertical_options(self):
        return {
            "decennial": {
                "tract": {"NTA": self.tract_to_nta, "cd": self.tract_to_cd},
                "block": {
                    "cd_fp_500": self.block_to_cd_fp500,
                    "cd_fp_100": self.block_to_cd_fp100,
                    "cd_park_access": self.block_to_cd_park_access,
                },
            },
            "acs": {
                "tract": {"NTA": self.tract_to_nta, "cd": self.tract_to_cd},
                "block group": {
                    "cd_fp_500": self.block_group_to_cd_fp500,
                    "cd_fp_100": self.block_group_to_cd_fp100,
                    "cd_park_access": self.block_group_to_cd_park_access,
                },
            },
        }
