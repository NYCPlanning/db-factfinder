from factfinder.main import Pff
from factfinder.median import get_median, get_median_moe
import os
import pandas as pd
import itertools
import sys
sys.path.append('../')

pff = Pff(
    api_key=os.environ['API_KEY'], 
    year = 2018
)


geography = ['city', 'tract', 'NTA', 'cd']
dfs = []

for domain in ['demographic','economic','housing','social']:
    variables = [i['pff_variable'] for i in pff.metadata if i['domain'] == domain][0:5]
    for var, geo in itertools.product(variables, geography):
        try:
            dfs.append(pff.calculate(var, geo).assign(domain=domain))
        except:
            print(var, geo)
df = pd.concat(dfs)
df.to_csv(sys.stdout, index=False)