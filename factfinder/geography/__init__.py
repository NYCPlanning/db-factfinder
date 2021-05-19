import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd
from cached_property import cached_property


class AggregatedGeography:
    def __init__(self):
        self.year = 2020

    @cached_property
    def lookup_geo(self):
        # find the current decennial year based on given year
        lookup_geo = pd.read_csv(
            Path(__file__).parent.parent
            / f"data/lookup_geo/{self.year}/lookup_geo.csv",
            dtype="str",
        )
        lookup_geo["geoid_block"] = lookup_geo.county_fips + lookup_geo.ctcb2010
        lookup_geo["geoid_block_group"] = lookup_geo.geoid_block.apply(
            lambda x: x[0:12]
        )
        lookup_geo["geoid_tract"] = lookup_geo.county_fips + lookup_geo.ct2010
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

    def tract_to_cdta(self, df):
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
    def options(self):
        return {
            "acs": {"tract": {"NTA": self.tract_to_nta, "CDTA": self.tract_to_cdta}}
        }

    @cached_property
    def aggregated_geography(self) -> list:
        list3d = [[list(k.keys()) for k in i.values()] for i in self.options.values()]
        list2d = itertools.chain.from_iterable(list3d)
        return list(set(itertools.chain.from_iterable(list2d)))

    # def assign_geotype(self, geoid):
    #     # NTA
    #     if geoid[:2] in ["MN", "QN", "BX", "BK", "SI"]:
    #         return "NTA2010"
    #     # Community District (PUMA)
    #     elif geoid[:2] == "79":
    #         return "PUMA2010"
    #     # Census tract (CT2010)
    #     elif geoid[:2] == "14":
    #         return "CT2010"
    #     # Boro
    #     elif geoid[:2] == "05":
    #         return "Boro2010"
    #     # City
    #     elif geoid[:2] == "16":
    #         return "City2010"
