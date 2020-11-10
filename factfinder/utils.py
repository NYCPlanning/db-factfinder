import pandas as pd
from pathlib import Path
import numpy as np
import math

outliers = [
    999999999,
    333333333,
    222222222,
    666666666,
    888888888,
    555555555,
    -999999999,
    -333333333,
    -222222222,
    -666666666,
    -888888888,
    -555555555,
]


def get_c(e, m):
    if e == 0:
        return np.nan
    else:
        return m / 1.645 / e * 100


def get_p(e, agg_e):
    if agg_e == 0:
        return np.nan
    else:
        return e / agg_e * 100


def get_z(e, m, p, agg_e, agg_m):
    if p == 0:
        return np.nan
    elif p == 100:
        return np.nan
    elif agg_e == 0:
        return np.nan
    elif m ** 2 - (e * agg_m / agg_e) ** 2 < 0:
        return math.sqrt(m ** 2 + (e * agg_m / agg_e) ** 2) / agg_e * 100
    else:
        return math.sqrt(m ** 2 - (e * agg_m / agg_e) ** 2) / agg_e * 100


def format_geoid(geoid):
    fips_lookup = {
        "05": "2",
        "47": "3",
        "61": "1",
        "81": "4",
        "85": "5",
    }
    # NTA
    if geoid[:2] in ["MN", "QN", "BX", "BK", "SI"]:
        return geoid
    # Community District (PUMA)
    elif geoid[:2] == "79":
        return geoid[-4:]
    # Census tract (CT2010)
    elif geoid[:2] == "14":
        boro = fips_lookup.get(geoid[-8:-6])
        return boro + geoid[-6:]
    # Boro
    elif geoid[:2] == "05":
        return fips_lookup.get(geoid[-2:])
    # City
    elif geoid[:2] == "16":
        return 0


def assign_geotype(geoid):
    # NTA
    if geoid[:2] in ["MN", "QN", "BX", "BK", "SI"]:
        return "NTA2010"
    # Community District (PUMA)
    elif geoid[:2] == "79":
        return "PUMA2010"
    # Census tract (CT2010)
    elif geoid[:2] == "14":
        return "CT2010"
    # Boro
    elif geoid[:2] == "05":
        return "Boro2010"
    # City
    elif geoid[:2] == "16":
        return "City2010"
