import itertools
import math
from functools import cached_property, wraps
from pathlib import Path

import numpy as np
import pandas as pd


class AggregatedGeography:
    def __init__(self):
        pass

    @cached_property
    def lookup_geo(self):
        # find the current decennial year based on given year
        lookup_geo = pd.read_csv(
            Path(__file__).parent.parent
            / f"data/lookup_geo/2010_to_2020/lookup_geo.csv",
            dtype="str",
        )
        # Create geoid_tract
        lookup_geo["county_fips"] = lookup_geo.geoid20.apply(lambda x: str(x)[:5])
        lookup_geo["geoid_tract"] = lookup_geo.geoid20.apply(lambda x: str(x)[:11])

        return lookup_geo

    @cached_property
    def ratio(self):
        ratio = pd.read_csv(
            Path(__file__).parent.parent / f"data/lookup_geo/2010_to_2020/ratio.csv",
            dtype="str",
        )
        ratio["ratio"] = ratio.Ratio10CTpartTo20CT.astype(float)
        ratio["geoid_ct2010"] = "360" + ratio["BoroCT2010"].str.pad(width=8, fillchar='0')
        ratio["geoid_ct2020"] = "360" + ratio["BoroCT2020"].str.pad(width=8, fillchar='0')
        return ratio[["geoid_ct2010", "geoid_ct2020", "ratio"]]

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

    @staticmethod
    def agg_moe(x):
        return math.sqrt(sum([i ** 2 for i in x]))

    @staticmethod
    def convert_moe(e_2010, m_2010, e_2020, ratio):
        if ratio == 1:
            return m_2010
        elif e_2020 == 0:
            return None
        elif (ratio**(0.56901))*7.96309 >= 1:
            return m_2010
        else:
            return ((ratio**(0.56901))*7.96309) * m_2010

    def ct2010_to_ct2020(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        this function will translate a dataframe from ct2010 to ct2020
        by multiplying a ratio on E/M
        """
        df = df.merge(
            self.ratio[["geoid_ct2010", "geoid_ct2020", "ratio"]],
            how="left",
            right_on="geoid_ct2010",
            left_on="census_geoid",
        )
        df["e_2010"] = df.e
        df["m_2010"] = df.m
        df.e = df.e * df.ratio
        df.m = df.apply(lambda row : self.convert_moe(row["e_2010"],
                     row["m_2010"], row["e"], row["ratio"]), axis = 1)
        
        output = df.rename(columns={"census_geoid":"input_geoid", "geoid_ct2020": "census_geoid"})
        output["geotype"] = "CT2020"

        return output[["input_geoid","census_geoid", "pff_variable", "geotype", "e", "m"]]

    def tract_to_nta(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to translate 2010 tract data to 2020 tract data,
        then aggregate to NTA2020 level
        """
        df = self.ct2010_to_ct2020(df)
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

    def tract_to_cdta(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Function to translate 2010 tract data to 2020 tract data,
        then aggregate to CDTA level
        """
        df = self.ct2010_to_ct2020(df)
        df = df.merge(
            self.lookup_geo[["geoid_tract", "cdta"]].drop_duplicates(),
            how="left",
            right_on="geoid_tract",
            left_on="census_geoid",
        )
        output = AggregatedGeography.create_output(df, "cdta")
        output["pff_variable"] = df["pff_variable"].to_list()[0]
        output["geotype"] = "CDTA"
        return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]

    @cached_property
    def options(self):
        """
        Options will register all the translator
        functions from one geography to another
        defined above
        """
        return {
            "acs": {"tract": {"NTA": self.tract_to_nta, "CDTA": self.tract_to_cdta, "CT20":self.ct2010_to_ct2020}}
        }

    @cached_property
    def aggregated_geography(self) -> list:
        """
        this will return a list of aggregated geography
        e.g. ['NTA', 'CDTA' ...]
        """
        list3d = [[list(k.keys()) for k in i.values()] for i in self.options.values()]
        list2d = itertools.chain.from_iterable(list3d)
        return list(set(itertools.chain.from_iterable(list2d)))
