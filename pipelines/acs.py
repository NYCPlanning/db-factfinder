import argparse
import logging
import os
import sys
from typing import Tuple

import pandas as pd
from pathos.pools import ProcessPool

from factfinder.calculate import Calculate

from . import API_KEY


def _calculate(args):
    var, domain, geo, calculate = args
    try:
        df = calculate(var, geo).assign(domain=domain)
        logging.info(f"✅ SUCCESS: {var}\t{geo}")
        return df
    except:
        e = sys.exc_info()[0]
        logging.ERROR(f"\n\n⛔️ FAILURE: {var}\t{geo}\n{e}\n\n")


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
    pool = ProcessPool(nodes=10)

    output_folder = f".output/acs/year={year}/geography={geography}"
    os.makedirs(output_folder, exist_ok=True)
    logging.basicConfig(
        filename=f".output/acs/year={year}/geography={geography}/error.log",
        encoding="utf-8",
        level=logging.ERROR,
    )
    # Initialize pff instance
    calculate = Calculate(api_key=API_KEY, year=year, source="acs", geography=geography)

    # Declare geography and variables involved in this caculation
    geogs = ["NTA", "CDTA", "CT20", "city", "borough"]
    if geography != "2010_to_2020":
        geogs.extend(["tract"])
    domains = ["demographic", "economic", "housing", "social"]
    variables = [
        (i["pff_variable"], i["domain"], j, calculate)
        for i in calculate.meta.metadata
        for j in geogs
        if i["domain"] in domains
    ]

    # Loop through calculations and collect dataframes in dfs
    dfs = pool.map(_calculate, variables)
    df = pd.concat(dfs)
    df.to_csv(f"{output_folder}/acs.csv", index=False)
