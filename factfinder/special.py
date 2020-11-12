import pandas as pd
import numpy as np


def pivot(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    dff = df.loc[:, ["census_geoid", "pff_variable", "e", "m"]].pivot(
        index="census_geoid", columns="pff_variable", values=["e", "m"]
    )
    pivoted = pd.DataFrame()
    pivoted["census_geoid"] = dff.index
    del df
    for i in base_variables:
        pivoted[i + "e"] = dff.e.loc[pivoted.census_geoid, i].to_list()
        pivoted[i + "m"] = dff.m.loc[pivoted.census_geoid, i].to_list()
    del dff
    return pivoted


def percapinc(df: pd.DataFrame, base_variables: list) -> pd.DataFrame:
    df = pivot(df, base_variables)
    df["e"] = df.agip15ple / df.pop_6e
    df["m"] = (
        1
        / df.pop_6e
        * np.sqrt(df.agip15plm ** 2 + (df.agip15ple * df.pop_6m / df.pop_6e) ** 2)
    )
    return df

special_variable_options = {
    'percapinc': percapinc
}