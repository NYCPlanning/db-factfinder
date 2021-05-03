import itertools
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from factfinder import Pool
from factfinder.main import Pff

# Load .env environmental variables for local runs
try:
    env_path = f"{Path(__file__).parent.parent.parent}/.env"
    load_dotenv(dotenv_path=env_path)
except:
    print(".env file is missing ...")

# Initialize pff instance
pff = Pff(api_key=os.environ["API_KEY"], year=2018)

# Declare geography and variables involved in this caculation
geography = ["city", "tract", "NTA", "cd"]
domains = ["demographic", "economic", "housing", "social"]
variables = [
    (i["pff_variable"], i["domain"]) for i in pff.metadata if i["domain"] in domains
]

# Function wrapper calculate for multiprocessing
def calculate(*args):
    (var, domain), geo = args[0]

    return pff.calculate(var, geo).assign(domain=domain)


# Loop through calculations and collect dataframes in dfs
dfs = []
for args in tqdm(list(itertools.product(variables, geography))):
    dfs.append(calculate(args))

# Concatenate dataframes and export to 1 large csv
df = pd.concat(dfs)
df.to_csv("pff_acs.csv", index=False)
