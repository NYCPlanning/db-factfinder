import itertools
import math
from functools import cached_property
from pathlib import Path

import numpy as np
import pandas as pd


class AggregatedGeography:
    def __init__(self):
        pass

    @cached_property
    def lookup_geo(self):
        # find the current decennial year based on given year
        lookup_geo = pd.read_csv(
            Path(__file__).parent.parent
            / f"data/lookup_geo/2010_to_2020/lookup_geo.csv",
            dtype="str",
        )
        return lookup_geo

    @cached_property
    def ratio(self):
        ratio = pd.read_csv(
            Path(__file__).parent.parent / f"data/lookup_geo/2010_to_2020/ratio.csv",
            dtype="str",
        )
        ratio.Ratio10CTpartTo20CT = ratio.Ratio10CTpartTo20CT.astype(float)
        return ratio

    def ct2010_to_ct2020(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        this function will translate a dataframe from ct2010 to ct2020
        by multiplying a ratio on E/M
        """
        return None
