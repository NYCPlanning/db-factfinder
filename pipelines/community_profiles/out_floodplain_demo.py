from functools import reduce
import pandas as pd
import os
from dotenv import load_dotenv
from factfinder.main import Pff

try:
    env_path = '../.env'
    load_dotenv(dotenv_path=env_path)
except:
    print('.env file is missing ...')

acs = Pff(api_key=os.environ['API_KEY'], year=int(os.environ['V_ACS'].split('-')[1]))
decennial = Pff(api_key=os.environ['API_KEY'], year=int(os.environ['V_DECENNIAL']))

dec_variable_mapping = [
    {'pff_variable': 'pop2010', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_pop'}},
    {'pff_variable': 'pop2010', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_pop'}},
    ]

acs_variable_mapping = [
    {'pff_variable': 'costburden', 'geotype': 'cd_fp_100', 'column_mapping': {'p': 'fp_100_cost_burden'}},
    {'pff_variable': 'costburden', 'geotype': 'cd_fp_500', 'column_mapping': {'p': 'fp_500_cost_burden'}},
    {'pff_variable': 'costburden', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_cost_burden_value'}},
    {'pff_variable': 'costburden', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_cost_burden_value'}},
    {'pff_variable': 'rentburden', 'geotype': 'cd_fp_100', 'column_mapping': {'p': 'fp_100_rent_burden'}},
    {'pff_variable': 'rentburden', 'geotype': 'cd_fp_500', 'column_mapping': {'p': 'fp_500_rent_burden'}},
    {'pff_variable': 'rentburden', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_rent_burden_value'}},
    {'pff_variable': 'rentburden', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_rent_burden_value'}},
    {'pff_variable': 'mdhhinc', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_mhhi'}},
    {'pff_variable': 'mdhhinc', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_mhhi'}},
    {'pff_variable': 'mortg', 'geotype': 'cd_fp_100', 'column_mapping': {'p': 'fp_100_permortg'}},
    {'pff_variable': 'mortg', 'geotype': 'cd_fp_500', 'column_mapping': {'p': 'fp_500_permortg'}},
    {'pff_variable': 'mortg', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_mortg_value'}},
    {'pff_variable': 'mortg', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_mortg_value'}},
    {'pff_variable': 'ownerocc', 'geotype': 'cd_fp_100', 'column_mapping': {'p': 'fp_100_ownerocc'}},
    {'pff_variable': 'ownerocc', 'geotype': 'cd_fp_500', 'column_mapping': {'p': 'fp_500_ownerocc'}},
    {'pff_variable': 'ownerocc', 'geotype': 'cd_fp_100', 'column_mapping': {'e': 'fp_100_ownerocc_value'}},
    {'pff_variable': 'ownerocc', 'geotype': 'cd_fp_500', 'column_mapping': {'e': 'fp_500_ownerocc_value'}},
    ]

def calculate(inputs, pff):
    pff_variable=inputs['pff_variable']
    geotype=inputs['geotype']
    column_mapping=inputs['column_mapping']
    return pff.calculate(pff_variable,geotype).rename(columns=column_mapping)[['census_geoid']+list(column_mapping.values())]

dfs = []

for i in acs_variable_mapping:
    try:
        df = calculate(i, acs)
        print(df.head())
        dfs.append(df)
    except:
        print("Didn't work:", i, acs)

for i in dec_variable_mapping:
    try:
        df = calculate(i, decennial)
        print(df.head())
        dfs.append(df)
    except:
        print("Didn't work:", i, decennial)

dff = reduce(lambda left,right: pd.merge(left,right, on=['census_geoid'],
                                            how='outer'), dfs)

dff.to_csv('output/fp_output.csv', index=False)
#dff.to_csv(sys.stdout, index=False)