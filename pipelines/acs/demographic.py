from factfinder.main import Pff
from factfinder.median import get_median, get_median_moe
import os
import pandas as pd
import itertools

pff = Pff(
    api_key=os.environ['API_KEY'], 
    year = 2018
)

geography = ['city', 'tract', 'NTA', 'cd']
variables = [i['pff_variable'] for i in pff.metadata if i['domain'] == 'demographic'][0:5]
dfs = []
for var, geo in itertools.product(variables, geography):
    try:
        dfs.append(pff.calculate(var, geo))
    except:
        print(var, geo)
df = pd.concat(dfs)