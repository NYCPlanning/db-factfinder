import pandas as pd
import numpy as np
from pathlib import Path
import math

lookup_geo = pd.read_csv(f"{Path(__file__).parent}/data/lookup_geo.csv", dtype="str")
lookup_geo["geoid_block"] = lookup_geo.county_fips + lookup_geo.ctcb2010
lookup_geo["geoid_block_group"] = lookup_geo.geoid_block.apply(lambda x: x[0:12])
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


def agg_moe(x):
    return math.sqrt(sum([i ** 2 for i in x]))


def create_output(df, colname):
    """
    this function will calculate the aggregated e and m 
    given colname we would like to aggregate over
    """
    return (
        df[[colname, "e"]]
        .groupby([colname])
        .sum()
        .merge(df[[colname, "m"]].groupby([colname]).agg(agg_moe), on=colname)
        .reset_index()
        .rename(columns={colname: "census_geoid"})
    )


def tract_to_nta(df):
    df = df.merge(
        lookup_geo[["geoid_tract", "nta"]].drop_duplicates(),
        how="left",
        right_on="geoid_tract",
        left_on="census_geoid",
    )
    output = create_output(df, "nta")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "NTA"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_group_to_cd_fp500(df):
    """
    500 yr flood plain aggregation for block group data (ACS)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_fp_500.isna(), ["geoid_block_group", "cd_fp_500"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block_group",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_fp_500")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_fp_500"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_group_to_cd_fp100(df):
    """
    100 yr flood plain aggregation for block group data (ACS)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_fp_100.isna(), ["geoid_block_group", "cd_fp_100"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block_group",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_fp_100")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_fp_100"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_group_to_cd_park_access(df):
    """
    walk-to-park access zone aggregation for block group data (acs)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_park_access.isna(), ["geoid_block_group", "cd_park_access"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block_group",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_park_access")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_park_access"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_to_cd_fp500(df):
    """
    500 yr flood plain aggregation for block data (decennial)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_fp_500.isna(), ["geoid_block", "cd_fp_500"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_fp_500")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_fp_500"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_to_cd_fp100(df):
    """
    100 yr flood plain aggregation for block data (decennial)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_fp_100.isna(), ["geoid_block", "cd_fp_100"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_fp_100")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_fp_100"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def block_to_cd_park_access(df):
    """
    walk-to-park access zone aggregation for block data (decennial)
    """
    df = df.merge(
        lookup_geo.loc[
            ~lookup_geo.cd_park_access.isna(), ["geoid_block", "cd_park_access"]
        ].drop_duplicates(),
        how="right",
        right_on="geoid_block",
        left_on="census_geoid",
    )
    output = create_output(df, "cd_park_access")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd_park_access"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


def tract_to_cd(df):
    """
    tract to cd
    """
    df = df.merge(
        lookup_geo[["geoid_tract", "cd"]].drop_duplicates(),
        how="left",
        right_on="geoid_tract",
        left_on="census_geoid",
    )
    output = create_output(df, "cd")
    output["pff_variable"] = df["pff_variable"].to_list()[0]
    output["geotype"] = "cd"
    return output[["census_geoid", "pff_variable", "geotype", "e", "m"]]


aggregate_vertical_options = {
    "decennial": {
        "tract": {"NTA": tract_to_nta, "cd": tract_to_cd},
        "block": {
            "cd_fp_500": block_to_cd_fp500,
            "cd_fp_100": block_to_cd_fp100,
            "cd_park_access": block_to_cd_park_access,
        },
    },
    "acs": {
        "tract": {"NTA": tract_to_nta, "cd": tract_to_cd},
        "block group": {
            "cd_fp_500": block_group_to_cd_fp500,
            "cd_fp_100": block_group_to_cd_fp100,
            "cd_park_access": block_group_to_cd_park_access,
        },
    },
}
