import itertools
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from multiprocessing import Pool
from factfinder.calculate import Calculate

# Load .env environmental variables for local runs
try:
    env_path = f"{Path(__file__).parent.parent.parent}/.env"
    load_dotenv(dotenv_path=env_path)
except:
    print(".env file is missing ...")

# Initialize pff instance
calculate = Calculate(api_key=os.environ["API_KEY"], year=2019, source="acs", geography="2010_to_2020")

# Declare geography and variables involved in this caculation
geography = ["city", "CT20", "NTA", "CDTA"]
domains = ["demographic", "economic", "housing", "social"]
variables = [
    (i["pff_variable"], i["domain"]) for i in calculate.meta.metadata if i["domain"] in domains
]

# Function wrapper calculate for multiprocessing
def calc(*args):
    (var, domain), geo = args[0]

    return calculate(var, geo).assign(domain=domain)


# Loop through calculations and collect dataframes in dfs
dfs = []
for args in tqdm(list(itertools.product(variables, geography))):
    dfs.append(calc(args))

# Concatenate dataframes and export to 1 large csv
df = pd.concat(dfs)
df.to_csv("pff_acs_converted.csv", index=False)