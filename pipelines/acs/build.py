from factfinder.main import Pff
from factfinder.multi import Pool
import os
import pandas as pd
import itertools
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm

# Declare geography and variables involved in this caculation
GEOGS = ['city', 'tract', 'NTA', 'cd']

# Function wrapper calculate for multiprocessing
def calculate(*args):
    (var, domain), geo = args[0]
    try:
        return pff.calculate(var, geo).assign(domain=domain)
    except:
        print(var, geo, domain)

if __name__ == "__main__":
    # Domain should be one of ['demographic','economic','housing','social']
    domain = sys.argv[1]

    # Load .env environmental variables for local runs
    try:
        env_path = f'{Path(__file__).parent.parent.parent}/.env'
        load_dotenv(dotenv_path=env_path)
    except:
        print('.env file is missing ...')

    # Initialize pff instance
    pff = Pff(api_key=os.environ['API_KEY'], year = 2018)

    # Create set of variables to loop through
    variables = [(i['pff_variable'], i['domain']) for i in pff.metadata if i['domain'] == domain]

    # Loop through calculations and collect dataframes in dfs
    for args in tqdm(list(itertools.product(variables, GEOGS))):    
        dfs.append(calculate(args))
    
    # Concatenate dataframes and export to domain-specific csv
    df = pd.concat(dfs)
    df.to_csv(f'pff_acs_{domain}.csv', index=False)