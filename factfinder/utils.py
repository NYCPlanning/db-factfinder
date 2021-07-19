import math
import os
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

def get_median(ranges, row):
    ordered = list(ranges.keys())
    N = row[ordered].sum()
    C = 0
    i = 0
    while C < N / 2 and i <= len(ranges.keys()) - 1:
        # Calculate cumulative frequency until half of all units are accounted for
        C += row[ordered[i]]
        i += 1
    i = i - 1
    if i == 0:
        # N/2 is in first bin
        median = list(ranges.values())[0][1]
    elif C == 0.0:
        # Cumulative frequency is 0
        median = 0.0
    elif i == len(ranges.keys()) - 1:
        # N/2 is in top range
        median = list(ranges.values())[-1][0]
    else:
        # N/2 is in range i
        # C_i-1: Cumulative frequency up to bin below N/2
        C = C - row[ordered[i]]
        # L_i: Lower boundary of median group
        L = ranges[ordered[i]][0]
        # F_i: Frequency within median group
        F = row[ordered[i]]
        # W_i: Width of median group
        W = ranges[ordered[i]][1] - ranges[ordered[i]][0]
        # Calculate median within middle bin using linear interpolation
        median = L + (N / 2 - C) * W / F
    return median


def get_median_moe(ranges, row, DF=1.1):
    md = row["e"]
    if md >= list(ranges.values())[-1][0]:
        # Median is above top bin lower value
        return np.nan
    else:
        ordered = list(ranges.keys())
        B = row[ordered].sum()
        if B == 0:
            # Size of base is zero
            return np.nan
        else:
            # Calculate cumulative distribution (percents)
            cumm_dist = list(np.cumsum(row[ordered]) / B * 100)

            # Calculate SE of a 50% proportion
            se_50 = DF * (((93 / (7 * B)) * 2500)) ** 0.5

            if se_50 >= 50:
                return np.nan
            else:
                # Calculate 1SE interval around 50% proportion
                p_lower = 50 - se_50
                p_upper = 50 + se_50

                # Identify bins containing lower and upper bounds of 1SE interval
                lower_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_lower])
                upper_bin = min([cumm_dist.index(i) for i in cumm_dist if i > p_upper])

                if lower_bin >= len(ordered) - 1:
                    # Lower bound is within the top bin
                    return np.nan
                else:
                    if lower_bin == upper_bin:
                        # Both bounds are within the same bin

                        # Calculate smallest value within the bin
                        A1 = min(ranges[ordered[lower_bin]])
                        # Calculate largest value within the bin
                        A2 = min(ranges[ordered[lower_bin + 1]])
                        # Cumulative percent of units less than smallest value 
                        C1 = cumm_dist[lower_bin - 1]
                        # Cumulative percent of units less than largest value 
                        C2 = cumm_dist[lower_bin]

                        # Calculate CI of median
                        lowerbound = (p_lower - C1) * (A2 - A1) / (C2 - C1) + A1
                        upperbound = (p_upper - C1) * (A2 - A1) / (C2 - C1) + A1

                    else:
                        # Bounds are within different bins

                        if lower_bin == 0:
                            # Lower bin is bottom bin

                            # Calculate smallest value in the lower bin
                            A1_l = min(ranges[ordered[lower_bin]])
                            # Calculate largest value in the lower bin
                            A2_l = min(ranges[ordered[lower_bin + 1]])
                            # Cumulative percent of units less than lower bin smallest value
                            C1_l = 0.0
                            # Cumulative percent of units less than lower bin largest value
                            C2_l = cumm_dist[lower_bin]
                        else:
                            # Calculate smallest value in the lower bin
                            A1_l = min(ranges[ordered[lower_bin]])
                            # Calculate largest value in the lower bin
                            A2_l = min(ranges[ordered[lower_bin + 1]])
                            # Cumulative percent of units less than lower bin smallest value
                            C1_l = cumm_dist[lower_bin - 1]
                            # Cumulative percent of units less than lower bin largest value
                            C2_l = cumm_dist[lower_bin]
                        if upper_bin + 1 > len(ordered) - 1:
                            # Upper bin is top bin

                            # Calculate smallest value in the upper bin
                            A1_u = min(ranges[ordered[upper_bin]])
                            # Calculate largest value in the upper bin
                            A2_u = A1_u
                            # Cumulative percent of units less than upper bin smallest value
                            C1_u = cumm_dist[upper_bin - 1]
                            # Cumulative percent of units less than upper bin largest value
                            C2_u = cumm_dist[upper_bin]
                        else:
                            # Calculate smallest value in the upper bin
                            A1_u = min(ranges[ordered[upper_bin]])
                            # Calculate largest value in the upper bin
                            A2_u = min(ranges[ordered[upper_bin + 1]])
                            # Cumulative percent of units less than upper bin smallest value
                            C1_u = cumm_dist[upper_bin - 1]
                            # Cumulative percent of units less than upper bin largest value
                            C2_u = cumm_dist[upper_bin]

                        # Calculate CI of median
                        lowerbound = (p_lower - C1_l) * (A2_l - A1_l) / (
                            C2_l - C1_l
                        ) + A1_l
                        upperbound = (p_upper - C1_u) * (A2_u - A1_u) / (
                            C2_u - C1_u
                        ) + A1_u

                    # Use CI of median to derive MOE of median
                    return (upperbound - lowerbound) * 1.645 / 2


def rounding(df: pd.DataFrame, digits: int) -> pd.DataFrame:
    """
    Round c, e, m, p, z fields based on rounding digits from metadata
    """
    df["c"] = df["c"].round(1)
    df["e"] = df["e"].round(digits)
    df["m"] = df["m"].round(digits)
    df["p"] = df["p"].round(1)
    df["z"] = df["z"].round(1)
    return df


def write_to_cache(df: pd.DataFrame, path: str):
    """
    this function will cache a dataframe to a given path
    """
    if not os.path.isfile(path):
        df.to_pickle(path)
    return None
