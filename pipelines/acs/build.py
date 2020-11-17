from factfinder.main import Pff
from factfinder.multi import Pool
import os
import pandas as pd
import itertools
from pathlib import Path
from dotenv import load_dotenv

# Load .env environmental variables for local runs
try:
    env_path = f'{Path(__file__).parent.parent.parent}/.env'
    load_dotenv(dotenv_path=env_path)
except:
    print('.env file is missing ...')

# Initialize pff instance
pff = Pff(api_key=os.environ['API_KEY'], year = 2018)

# Declare geography and variables involved in this caculation
geography = ['city', 'tract', 'NTA', 'cd']
variables = [(i['pff_variable'], i['domain']) for i in pff.metadata[:10] if i['domain'] != 'decennial']

# Function wrapper calculate for multiprocessing
def calculate(*args):
    (var, domain), geo = args[0]
    return pff.calculate(var, geo).assign(domain=domain)

# Initialize Pool for multiprocessing and collect dataframes in dfs
with Pool(5) as pool:
    dfs=pool.map(calculate, itertools.product(variables, geography))

# Concatenate dataframes and export to 1 large csv
df = pd.concat(dfs)
df.to_csv('pff_acs.csv', index=False)