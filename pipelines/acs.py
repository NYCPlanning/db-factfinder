import argparse
import itertools
import os
import sys
from typing import Tuple

import pandas as pd
from tqdm import tqdm

from factfinder.calculate import Calculate

from . import API_KEY


def calc(*args):
    (var, domain), geo = args[0]
    return calculate(var, geo).assign(domain=domain)


def parse_args() -> Tuple[int, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-y", "--year", type=int, help="The ACS5 year, e.g. 2019 (2014-2018)"
    )
    parser.add_argument(
        "-g", "--geography", type=str, help="The geography year, e.g. 2010_to_2020"
    )
    args = parser.parse_args()
    return args.year, args.geography


if __name__ == "__main__":
    # Get ACS year
    year, geography = parse_args()

    # Initialize pff instance
    calculate = Calculate(api_key=API_KEY, year=year, source="acs", geography=geography)

    # Declare geography and variables involved in this caculation
    geogs = calculate.geo.aggregated_geography + ["city", "borough"]
    if geography != "2010_to_2020":
        geogs.extend(["tract"])
    domains = ["demographic", "economic", "housing", "social"]
    variables = [
        (i["pff_variable"], i["domain"])
        for i in calculate.meta.metadata
        if i["domain"] in domains
        and i["pff_variable"] in calculate.meta.median_variables
    ]

    # Loop through calculations and collect dataframes in dfs
    dfs = []
    for args in tqdm(list(itertools.product(variables, geogs))):
        try:
            df = calc(args)
            dfs.append(df)
        except:
            print(args)

    # Concatenate dataframes and export to 1 large csv
    output_folder = f".output/acs/year={year}/geography={geography}"
    df = pd.concat(dfs)
    os.makedirs(output_folder)
    df.to_csv(f"{output_folder}/acs.csv", index=False)
