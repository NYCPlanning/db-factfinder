import argparse
import os
import sys

import pandas as pd
import re

def parse_args() -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, help="Location of input ACS file")

    args = parser.parse_args()
    return args.input

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
        elif set(new_df.columns) == set(output_df.columns):
            output_df = pd.concat([output_df, new_df], ignore_index=True)
        else:
            print(field_name)
            print(domain)
    return output_df

def extract_field_names(df):
     return df.columns[2:].str[:-1].drop_duplicates()
     
def split_by_field_name(df, pff_field_name):
    return df.filter(regex=f"^(GeoType|GeoID|{ pff_field_name }(E|M|C|P|Z))$")

if __name__ == "__main__":
    # Get ACS year
    input = parse_args()

    excel_file = pd.ExcelFile("ACS1620Data_06-23-22.xlsx")
    data_frames = pd.read_excel(excel_file, sheet_name=['Dem1620', 'Social1620', 'Econ1620', 'Housing1620'])

    domains = {
        "Dem1620": "demographic",
        "Social1620": "social",
        "Econ1620": "economic",
        "Housing1620": "housing"
    }

    export_df = pd.DataFrame()
    for sheet_name, domain in domains.items():
        export_df = pd.concat([export_df, transform_dataframe(data_frames[sheet_name], domain)])

    export_df.rename(columns={"geotype": "labs_geotype", "geoid": "labs_geoid"}, inplace=True)
    export_df = export_df[['labs_geotype', 'labs_geoid', 'pff_variable', 'e', 'm', 'c', 'p', 'z', 'domain']]

    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output"
    os.makedirs(output_folder, exist_ok=True)
    export_df.to_csv(f"{output_folder}/acs.csv", index=False)

