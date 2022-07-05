import argparse
import os
from typing import Tuple

import pandas as pd
import re

def parse_args() -> Tuple[str, str]:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-y", "--year", type=str, help="The ACS5 year, e.g. 2019 (2014-2018)"
    )
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.year, args.geography

def pivot_field_name(df, field_name, domain):
    field_name_df = split_by_field_name(df, field_name)

    field_name_df.rename(columns=lambda column_name: re.sub(f"^{ field_name }(E|M|C|P|Z)$",r"\1", column_name).lower(), inplace=True)
    field_name_df['pff_variable'] = field_name.lower()
    field_name_df['domain'] = domain

    return field_name_df

def transform_dataframe(df, domain):
    df = strip_unnamed_columns(df)
    pff_field_names = extract_field_names(df)
    output_df = pd.DataFrame()

    for field_name in pff_field_names:

        new_df = pivot_field_name(df, field_name, domain)

        if output_df.empty:
            output_df = new_df
        else:
            output_df = pd.concat([output_df, new_df], ignore_index=True)
    return output_df

def extract_field_names(df):
    return df.columns[2:].str[:-1].drop_duplicates()
     
def split_by_field_name(df, pff_field_name):
    return df.filter(regex=f"^(GeoType|GeoID|{ pff_field_name }(E|M|C|P|Z))$")

def strip_unnamed_columns(df):
    return df.loc[:,~df.columns.str.match("Unnamed")]

def transform_all_dataframes(year):
    if year == '2010':
        sheet_name_suffix = '0610'
        inflated = "_Inflated"
    if year == '2020':
        sheet_name_suffix = '1620'
        inflated = ""

    input_file = f"factfinder/data/acs_1620_update/{year}/acs_{year}.xlsx"

    domains_sheets = [
        {
            'domain': 'demographic',
            'sheet_name': f'Dem{sheet_name_suffix}'
        }, 
        {
            'domain': 'social',
            'sheet_name': f'Social{sheet_name_suffix}'
        }, 
        {
            'domain': 'economic',
            'sheet_name': f'Econ{sheet_name_suffix}{inflated}'
        }, 
        {
            'domain': 'housing',
            'sheet_name': f'Housing{sheet_name_suffix}{inflated}'
        }
    ]
    
    dfs = pd.read_excel(input_file, sheet_name=None, engine='openpyxl')
    combined_df = pd.DataFrame()

    for domain_sheet in domains_sheets:
        combined_df = pd.concat([combined_df, transform_dataframe(dfs[domain_sheet['sheet_name']], domain_sheet['domain'])])

    combined_df.dropna(subset=['geotype'], inplace=True)

    return combined_df

def attach_base_variable(df, year):
    metadata_file = f"factfinder/data/acs/{year}/metadata.json"
    acs_variable_mapping = pd.read_json(metadata_file)[['base_variable', 'pff_variable']]

    return df.merge(acs_variable_mapping, how='left', on='pff_variable')


if __name__ == "__main__":
    # Get ACS year
    year, geography = parse_args()

    export_df = transform_all_dataframes(year)
    export_df = attach_base_variable(export_df, year)

    export_df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    export_df = export_df[['labs_geotype', 'labs_geoid', 'pff_variable', 'base_variable', 'e', 'm', 'c', 'p', 'z', 'domain']]

    output_folder = f".output/acs/year={year}/geography={geography}"

    os.makedirs(output_folder, exist_ok=True)
    export_df.to_csv(f"{output_folder}/acs_manual_update.csv", index=False)
