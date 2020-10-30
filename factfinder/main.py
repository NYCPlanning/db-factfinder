from census import Census
import json
import pandas as pd
from pathlib import Path


class Pff:
    def __init__(self, api_key, year=2018):
        self.c = Census(api_key)
        with open(f"{Path(__file__).parent}/data/metadata.json") as f:
            self.metadata = json.load(f)
        self.year = year
        self.state = 36
        self.counties = ["005", "081", "085", "047", "061"]

    def create_variable(self, pff_variable):
        meta = list(filter(lambda x: x["pff_variable"] == pff_variable, self.metadata))[
            0
        ]
        return Variable(meta)

    def calculate_variable(self, pff_variable, geotype=""):
        # 1. create variable
        v = self.create_variable(pff_variable)

        # 2. identify source
        if v.source == "profile":
            source = self.c.acs5dp
        if v.source == "subject":
            source = self.c.acs5st
        source = self.c.acs5

        # 3. pulling data from census site
        return self.download_variable(source, v, geotype)

    def download_variable(self, source, v, geotype):
        dfs = []
        variables = [i+'E' for i in v.acs_variable]+\
                    [i+'M' for i in v.acs_variable]

        for county in self.counties:
            dfs.append(
                pd.DataFrame(
                    source.get(
                        ("NAME", ",".join(variables)),
                        {
                            "for": "tract:*",
                            "in": f"state:{self.state} county:{county}",
                        },
                        year=self.year,
                    )
                )
            )
        df = pd.concat(dfs)
        df["pff_variable"] = v.pff_variable
        return df


class Variable:
    def __init__(self, kwargs):
        self.pff_variable = kwargs.get("pff_variable")
        self.acs_variable = kwargs.get("acs_variable")
        self.domain = kwargs.get("domain")
        self.base_variable = kwargs.get("base_variable")
        self.rounding = kwargs.get("rounding")
        self.source = kwargs.get("source")
        self.median = kwargs.get("median")
        self.range = kwargs.get("range")
        self.design_factor = kwargs.get("design_factor")
        self.meta = kwargs
