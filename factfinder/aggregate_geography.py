import pandas as pd
from pathlib import Path
import math

lookup_geo = pd.read_csv(f"{Path(__file__).parent}/data/lookup_geo.csv", dtype="str")
lookup_geo["geoid_block"] = lookup_geo.fips_boro + lookup_geo.ctcb2010
lookup_geo["geoid_tract"] = lookup_geo.fips_boro + lookup_geo.ct2010


def agg_moe(x):
    return math.sqrt(sum([i ** 2 for i in x]))

def aggregate_nta(df):
    df = df.merge(
            lookup_geo[['geoid_tract', 'nta']].drop_duplicates(),
            how='left', right_on='geoid_tract', left_on='acs_geoid')
    output = df[['nta', 'e']].groupby(['nta']).sum().merge(
        df[['nta', 'm']].groupby(['nta']).agg(agg_moe),
        on = 'nta'
    ).reset_index().rename(columns={'nta':'acs_geoid'})
    output['pff_variable'] = df['pff_variable'].to_list()[0]
    output['geotype'] = 'NTA'
    return output[['acs_geoid', 'pff_variable', 'geotype', 'e', 'm']]