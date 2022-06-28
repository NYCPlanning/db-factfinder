import argparse
import os
import sys
from typing import Tuple

import pandas as pd
import re

def parse_args() -> Tuple[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="Location of input ACS file")
    parser.add_argument(
        "-y", "--year", type=str, help="The ACS5 year, e.g. 2019 (2014-2018)"
    )
    args = parser.parse_args()
    return args.input, args.year

def transform_dataframe(df, domain):
    pff_field_names = extract_field_names(df)
    output_df = pd.DataFrame()
    for field_name in pff_field_names:
        new_df = split_by_field_name(df, field_name)
        new_df = new_df.rename(columns=lambda x: re.sub(f"^{ field_name }(E|M|C|P|Z)$",r"\1",x).lower())
        new_df['pff_variable'] = field_name.lower()
        new_df['domain'] = domain
        if output_df.empty:
            output_df = new_df
        else:
            output_df = pd.concat([output_df, new_df], ignore_index=True)
    return output_df

def extract_field_names(df):
     return df.columns[2:].str[:-1].drop_duplicates()
     
def split_by_field_name(df, pff_field_name):
    return df.filter(regex=f"^(GeoType|GeoID|{ pff_field_name }(E|M|C|P|Z))$")

if __name__ == "__main__":
    # Get ACS year
    input_file, year = parse_args()

    excel_file = pd.ExcelFile(input_file)
    data_frames = pd.read_excel(excel_file, sheet_name=[0, 1, 2, 3])
    domains = ['demographic', 'social', 'economic', 'housing']

    export_df = pd.DataFrame()
    for idx, domain in enumerate(domains):
        export_df = pd.concat([export_df, transform_dataframe(data_frames[idx], domain)])

    export_df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    export_df = export_df[['labs_geotype', 'labs_geoid', 'pff_variable', 'e', 'm', 'c', 'p', 'z', 'domain']]

    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output/{ year }"
    os.makedirs(output_folder, exist_ok=True)
    export_df.to_csv(f"{output_folder}/acs.csv", index=False)

