import itertools
import os

import pandas as pd

from factfinder.main import Pff
from factfinder.median import get_median, get_median_moe

pff = Pff(api_key=os.environ["API_KEY"], year=2018)

geography = ["city", "tract", "NTA", "cd"]
variables = [i["pff_variable"] for i in pff.metadata if i["domain"] == "demographic"][
    0:5
]
dfs = []
for var, geo in itertools.product(variables, geography):
    dfs.append(pff.calculate(var, geo))

df = pd.concat(dfs)
