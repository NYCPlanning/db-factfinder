from functools import reduce
import requests
import pandas as pd
import json
import sys
from pathlib import Path

ALL_YEARS = ['2018', '2019']    
PREFIX = {
            "D": "profile/",
            "S": "subject/",
            "P": "",
            "B": "",
            "C": "",
        }

def collapse_var_types(df:pd.DataFrame, year:str) -> pd.DataFrame:
    df[f'census_variable_{year}'] = df[f'census_variable_{year}'].str.replace("PE|E", "", regex=True)
    df['variable_name'] = df[f'census_variable_{year}'].str.replace("Estimate|Percent", "", regex=True)
    return df.drop_duplicates()

def get_metadata(year:str, prefix:str) -> pd.DataFrame:
    group = PREFIX.get(prefix, '')
    df = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs5/{group}variables.json')
    df = df['variables'].apply(pd.Series).reset_index()
    df = df.loc[df['index'].str.startswith(f"{prefix}") & ~df['index'].str.contains("PR")]
    return df[['label','index']].rename(columns={'index':f'census_variable_{year}', 'label':'variable_name'})

def get_year(year:str) -> pd.DataFrame:
    
    """
    # Get DP data
    df_dp = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs5/profile/variables.json')
    df_dp = df_dp['variables'].apply(pd.Series).reset_index().rename(columns={'index':f'census_variable_{year}', 'label':'variable_name'})
    df_dp = df_dp.loc[df_dp['index'].str.startswith("DP") & ~df_dp['index'].str.contains("PR")]

    # Get B/C data
    df_bc = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs5/variables.json')
    df_bc = df_bc['variables'].apply(pd.Series).reset_index().rename(columns={'index':f'census_variable_{year}', 'label':'variable_name'})

    df_b = df_bc.loc[df_bc['index'].str.startswith("B") & ~df_bc['index'].str.contains("PR")]
    df_c = df_bc.loc[df_bc['index'].str.startswith("C") & ~df_bc['index'].str.contains("PR")]

    # Get S data
    df_s = pd.read_json(f'https://api.census.gov/data/{year}/acs/acs5/subject/variables.json')
    df_s = df_s['variables'].apply(pd.Series).reset_index().rename(columns={'index':f'census_variable_{year}', 'label':'variable_name'})
    df_s = df_s.loc[df_s['index'].str.startswith("S") & ~df_s['index'].str.contains("PR")]

    df = df_dp.append([df_b, df_c, df_s])
    df = df[['label','index']]

    return df
    """

def get_mult_years(years_list:list) -> pd.DataFrame:
    dfs = []
    for year in years_list:
        dfs.append(get_year(year))

    df = reduce(lambda left,right: pd.merge(left,right, on=['variable_name'],
                                                how='left'), dfs)
    return df

def acs_variable_change(year:str) -> pd.DataFrame:
    last_year = str(int(year)-1)
    year_list = [year, last_year]
    df = get_mult_years(year_list)
    df_changed = df[df[f'census_variable_{year}'] != df[f'census_variable_{last_year}']]
    return df_changed

if __name__ == "__main__":
    year = sys.argv[1]
    df = one_yr_change(year)
    df.to_csv(f'census_var_changes_{year}.csv', index=False)
