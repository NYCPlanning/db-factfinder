import argparse
import os
from typing import Tuple

import pandas as pd
import re

def parse_args() -> Tuple[str, str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="Location of input ACS file")
    parser.add_argument(
        "-y", "--year", type=str, help="The ACS5 year, e.g. 2019 (2014-2018)"
    )
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.input, args.year, args.geography

def transform_dataframe(df, domain):
    base_field_names = extract_field_names(df)
    output_df = pd.DataFrame()

    for field_name in base_field_names:
        new_df = split_by_field_name(df, field_name)
        new_df = new_df.rename(columns=lambda x: re.sub(f"^{ field_name }(E|M|C|P|Z)$",r"\1",x).lower())
        new_df['base_variable'] = field_name.lower()
        new_df['domain'] = domain
        
        if output_df.empty:
            output_df = new_df
        else:
            output_df = pd.concat([output_df, new_df], ignore_index=True)

    return output_df

def extract_field_names(df):
    return df.columns[2:].str[:-1].drop_duplicates()
     
def split_by_field_name(df, base_field_name):
    return df.filter(regex=f"^(GeoType|GeoID|{ base_field_name }(E|M|C|P|Z))$")

if __name__ == "__main__":
    # Get ACS year
    input_file, year, geography = parse_args()

    data_frames = pd.read_excel(input_file, sheet_name=[0, 1, 2, 3], engine='openpyxl')
    domains = ['demographic', 'social', 'economic', 'housing']
    export_df = pd.DataFrame()

    for idx, domain in enumerate(domains):
        export_df = pd.concat([export_df, transform_dataframe(data_frames[idx], domain)])

    export_df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    export_df = export_df[['labs_geotype', 'labs_geoid', 'base_variable', 'e', 'm', 'c', 'p', 'z', 'domain']]

    output_folder = f".output/acs/year={year}/geography={geography}"

    os.makedirs(output_folder, exist_ok=True)
    export_df.to_csv(f"{output_folder}/acs_manual_update.csv", index=False)

