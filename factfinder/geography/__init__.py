import math

import numpy as np


def agg_moe(x):
    return math.sqrt(sum([i ** 2 for i in x if i or not np.isnan(i)]))
