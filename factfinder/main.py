from census import Census
import json
import pandas as pd
from pathlib import Path
from .variable import Variable
from .database import Database
from .utils import get_c, get_p, get_z

class Pff:
    def __init__(self, api_key, database, year=2018):
        self.c = Census(api_key)
        with open(f"{Path(__file__).parent}/data/metadata.json") as f:
            self.metadata = json.load(f)
        self.year = year
        self.state = 36
        self.counties = ["005", "081", "085", "047", "061"]
        self.database = Database(database)

    @property
    def base_variables(self):
        base_var = set([i["base_variable"] for i in self.metadata])
        return [i for i in self.metadata if i["pff_variable"] in base_var]

    @property
    def median_variables(self):
        return [i for i in self.metadata if i["median"]]

    def create_variable(self, pff_variable):
        meta = list(filter(lambda x: x["pff_variable"] == pff_variable, self.metadata))[
            0
        ]
        return Variable(meta)

    def calculate_variable(self, pff_variable, geotype="tract"):
        # 1. create variable
        v = self.create_variable(pff_variable)

        # 2. identify source
        if v.source == "profile":
            source = self.c.acs5dp
        if v.source == "subject":
            source = self.c.acs5st
        source = self.c.acs5

        # 3. pulling data from census site
        df = self.aggregate_variable(source, v, geotype)
        return df

    def aggregate_variable(self, source, v, geotype):
        # Create Variables
        E_variables = [i + "E" for i in v.acs_variable]
        M_variables = [i + "M" for i in v.acs_variable]
        acs_variables = E_variables+M_variables
        geoqueries = self.get_geoquery(geotype)
        df = self.download(source, acs_variables, geoqueries)

        # Aggregate variables horizontally
        df["pff_variable"] = v.pff_variable
        df['geotype'] = geotype
        df['e'] = df[E_variables].sum(axis=1)
        df['m'] = (df[M_variables]**2).sum(axis=1)**0.5

        # Create geoid
        if geotype == 'tract':
            df['acs_geoid'] = df['state'] + df['county'] + df['tract']
        elif geotype == 'borough':
            df['acs_geoid'] = df['state'] + df['county']
        elif geotype == 'city':
            df['acs_geoid'] = df['state'] + df['place']
        elif geotype == 'block group':
            df['acs_geoid'] = df['state'] + df['county'] + df['tract'] + df['block group']
        return df[['acs_geoid', 'pff_variable', 'geotype', 'e', 'm']]
    
    def download(self, source, variables, geoqueries) -> pd.DataFrame: 
        dfs = []
        for geoquery in geoqueries:
            dfs.append(pd.DataFrame(
                source.get(
                ('NAME', ','.join(variables)),
                geoquery,year=self.year)
            ))
        return pd.concat(dfs)

    def get_geoquery(self, geotype) -> list:
        if geotype == 'tract':
            return [
                {
                    "for": "tract:*",
                    "in": f"state:{self.state} county:{county}",
                } for county in self.counties
            ]
        elif geotype == 'borough':
            return [
                {
                    "for": f"county:{county}",
                    "in": f"state:{self.state}",
                } for county in self.counties
            ]
        elif geotype == 'city': 
            return [
                {
                    "for": "place:51000",
                    "in": f"state:{self.state}",
                }
            ]
        elif geotype == 'block group':
            return [
                {
                    "for": 'block group:*',
                    "in": f"state:{self.state} county:{county}",
                } for county in self.counties
            ] 