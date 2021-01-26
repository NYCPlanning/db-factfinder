from functools import reduce
import requests
import pandas as pd
import json
import sys
    
PREFIX = {
            "DP": "profile/",
            "S": "subject/",
            "B": "",
            "C": "",
        }

def collapse_var_types(df:pd.DataFrame, year:str) -> pd.DataFrame:
    """
    Takes lookup of variable IDs and their associated names
    and removes distinction between estimate and percent vars
    to match the variable format in factfinder metadata.
    """
    df[f"census_variable_{year}"] = df[f"census_variable_{year}"].str.replace("PE|E", "", regex=True)
    df["variable_name"] = df["variable_name"].str.replace("Estimate!!|Percent!!|Percent Estimate!!", "", regex=True)
    df["variable_name"] = df["variable_name"].str.replace(":", "", regex=True)
    df[f"table_{year}"] = df[f"census_variable_{year}"].str.split("_").str[0]
    return df.drop_duplicates()

def get_api_vars(year:str, prefix:str) -> pd.DataFrame:
    """
    Given a year and variable prefix, pull a lookup between variable ID and name
    from the census API
    """
    group = PREFIX.get(prefix, "")
    df = pd.read_json(f"https://api.census.gov/data/{year}/acs/acs5/{group}variables.json")
    df = df["variables"].apply(pd.Series).reset_index()
    df = df.loc[df["index"].str.startswith(f"{prefix}") & ~df["index"].str.contains("PR")]
    return df[["label","index"]].rename(columns={"index":f"census_variable_{year}", "label":"variable_name"})

def get_year(year:str) -> pd.DataFrame:
    """
    For a given year, pulls and formats API variable lookups for
    the ACS variables with prefixes DP, S, B, or C. Combines into a 
    single DataFrame.
    """
    dfs = []
    for prefix in list(PREFIX.keys()):
        df = get_api_vars(year, prefix)
        dfs.append(collapse_var_types(df, year))
    return pd.concat(dfs, ignore_index=True)

def get_mult_years(years_list:list) -> pd.DataFrame:
    """
    For a list of years, pulls and formats API variable lookups for
    the ACS variables with prefixes DP, S, B, or C by calling get_year.

    Joins subsequent years' data as new columns, using a left join on
    variable name.
    """
    dfs = []
    for year in years_list:
        dfs.append(get_year(year))

    df = reduce(lambda left,right: pd.merge(left,right, on=["variable_name"],
                                                how="left"), dfs)
    return df

def get_pff_metadata(year:str) -> pd.DataFrame:
    """
    Open PFF metadata for the given year, convert to a DataFrame
    with one record per census_variable
    """
    with open(f"data/acs/{year}/metadata.json") as f:
        df = pd.DataFrame(json.load(f))
    df = df.explode("census_variable")
    return df

def acs_variable_change(year:str, change_only:bool=True) -> pd.DataFrame:
    """
    Calls get_mult_years to get lookups between a given year and
    the one that preceeds it. Merges with pff metadata.
    If change_only, outputs records where the table is the same 
    for a given variable name, but the variable ID has changed.
    Else, outputs all joined records from pff metadata.
    """
    last_year = str(int(year)-1)
    year_list = [last_year, year]
    df = get_mult_years(year_list)
    pff_metadata = get_pff_metadata(year)
    df = pff_metadata.merge(df, 
                        how="inner",
                        left_on="census_variable", 
                        right_on=f"census_variable_{last_year}")
    if change_only:
        df_changed = df.loc[(df[f"census_variable_{year}"] != df[f"census_variable_{last_year}"]) &
                    (df[f"table_{year}"] == df[f"table_{last_year}"]) |
                    (df[f"table_{year}"].isna())]
        return df_changed
    else:
        return df

if __name__ == "__main__":
    year = sys.argv[1]
    df = acs_variable_change(year)
    df.to_csv(f"data/acs/{year}/var_changes_{year}.csv", index=False)
