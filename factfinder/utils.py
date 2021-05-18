import math
from pathlib import Path

import numpy as np
import pandas as pd

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
    fips_lookup = {"05": "2", "47": "3", "61": "1", "81": "4", "85": "5"}
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


def get_median(ranges, row):
    ordered = list(ranges.keys())
    N = row[ordered].sum()
    C = 0
    i = 0
    while C <= N / 2 and i <= len(ranges.keys()) - 1:
        C += int(row[ordered[i]])
        i += 1
    i = i - 1
    if i == 0:
        median = list(ranges.values())[0][1]
    elif C == 0:
        median = 0
    elif i == len(ranges.keys()) - 1:
        median = list(ranges.values())[-1][0]
    else:
        C = C - int(row[ordered[i]])
        L = ranges[ordered[i]][0]
        F = int(row[ordered[i]])
        W = ranges[ordered[i]][1] - ranges[ordered[i]][0]
        median = L + (N / 2 - C) * W / F
    return median


def get_median_moe(ranges, row, DF=1.1):
    md = row["e"]
    if md >= list(ranges.values())[-1][0]:
        return np.nan
    else:
        ordered = list(ranges.keys())
        B = row[ordered].sum()
        if B == 0:
            return np.nan
        else:
            cumm_dist = list(np.cumsum(row[ordered]) / B * 100)

            se_50 = DF * (((93 / (7 * B)) * 2500)) ** 0.5

            if se_50 >= 50:
                return np.nan
            else:
                p_lower = 50 - se_50
                p_upper = 50 + se_50

                lower_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_lower])
                upper_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_upper])

                if lower_bin >= len(ordered) - 1:
                    return np.nan
                else:
                    if lower_bin == upper_bin:
                        A1 = min(ranges[ordered[lower_bin]])
                        A2 = min(ranges[ordered[lower_bin + 1]])
                        C1 = cumm_dist[lower_bin - 1]
                        C2 = cumm_dist[lower_bin]
                        lowerbound = (p_lower - C1) * (A2 - A1) / (C2 - C1) + A1
                        upperbound = (p_upper - C1) * (A2 - A1) / (C2 - C1) + A1

                    else:

                        A1_l = min(ranges[ordered[lower_bin]])
                        A2_l = min(ranges[ordered[lower_bin + 1]])
                        C1_l = cumm_dist[lower_bin - 1]
                        C2_l = cumm_dist[lower_bin]

                        if upper_bin + 1 > len(ordered) - 1:
                            A1_u = min(ranges[ordered[upper_bin]])
                            A2_u = A1_u
                            C1_u = cumm_dist[upper_bin - 1]
                            C2_u = cumm_dist[upper_bin]
                        else:
                            A1_u = min(ranges[ordered[upper_bin]])
                            A2_u = min(ranges[ordered[upper_bin + 1]])
                            C1_u = cumm_dist[upper_bin - 1]
                            C2_u = cumm_dist[upper_bin]

                        lowerbound = (p_lower - C1_l) * (A2_l - A1_l) / (
                            C2_l - C1_l
                        ) + A1_l
                        upperbound = (p_upper - C1_u) * (A2_u - A1_u) / (
                            C2_u - C1_u
                        ) + A1_u

                    return (upperbound - lowerbound) * 1.645 / 2
