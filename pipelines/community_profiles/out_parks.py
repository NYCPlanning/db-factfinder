import pandas as pd
import os
from dotenv import load_dotenv
from factfinder.main import Pff

try:
    env_path = '../.env'
    load_dotenv(dotenv_path=env_path)
except:
    print('.env file is missing ...')

decennial = Pff(api_key=os.environ['API_KEY'], year=int(os.environ['V_DECENNIAL']))

df_with_access = decennial.calculate('pop2010', 'cd_park_access')
df_total = decennial.calculate('pop2010', 'cd')
df = df_with_access.merge(df_total, on='census_geoid', suffixes=('_access', '_total'))
df['per_access'] = df['e_access']/df['e_total']

df[['census_geoid','per_access']].to_csv('output/parks_output.csv', index=False)
#df[['census_geoid','per_access']].to_csv(sys.stdout, index=False)