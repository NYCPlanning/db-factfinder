import logging

import numpy as np

from . import LOGLEVEL

logging.basicConfig(level=LOGLEVEL)


class MedianMoe:
    def __init__(self, ranges, row, DF=1.1):
        self.md = row["e"]
        self.ordered = list(ranges.keys())
        self.ranges = ranges
        self.B = row[self.ordered].sum()
        self.se_50 = DF * (((93 / (7 * self.B)) * 2500)) ** 0.5
        self.p_lower = 50 - self.se_50
        self.p_upper = 50 + self.se_50
        self.cumm_dist = list(np.cumsum(row[self.ordered]) / self.B * 100)
        self.lower_bin = min(
            [self.cumm_dist.index(i) for i in self.cumm_dist if i > self.p_lower]
        )
        self.upper_bin = min(
            [self.cumm_dist.index(i) for i in self.cumm_dist if i > self.p_upper]
        )

    @property
    def cumm_dist_given_bin(self):
        return "\n\t".join(
            [
                f"{_bin}: {dist}"
                for _bin, dist in zip(list(self.ranges.values()), self.cumm_dist)
            ]
        )

    @property
    def first_non_zero_bin(self):
        return next((self.cumm_dist.index(i) for i in self.cumm_dist if i != 0), None)

    @staticmethod
    def get_bound(p, A1, A2, C1, C2):
        return (p - C1) * (A2 - A1) / (C2 - C1) + A1

    def base_case(self, _bin):
        # and not equal to first_non_zero_bin
        A1 = min(self.ranges[self.ordered[_bin]])
        A2 = min(self.ranges[self.ordered[_bin + 1]])
        C1 = self.cumm_dist[_bin - 1]
        C2 = self.cumm_dist[_bin]
        return A1, A2, C1, C2

    @property
    def lower_bound(self):
        # The default
        A1, A2, C1, C2 = self.base_case(self.lower_bin)

        if self.lower_bin == 0:
            logging.debug("lower_bin in bottom bin")
            C1 = 0.0
            C2 = self.cumm_dist[self.lower_bin]

        if self.lower_bin == self.first_non_zero_bin:
            logging.debug("lower_bin not in bottom bin and is the first none-zero bin")
            A1 = 0
            A2 = min(self.ranges[self.ordered[1]])

        logging.debug(
            f"""
            LOWER_BOUND:
            -----
            A1={A1}, A2={A2}, C1={C1}, C2={C2}
            """
        )

        return MedianMoe.get_bound(p=self.p_lower, A1=A1, A2=A2, C1=C1, C2=C2)

    @property
    def upper_bound(self):
        # The default
        A1, A2, C1, C2 = self.base_case(self.upper_bin)

        if self.upper_bin + 1 > len(self.ordered) - 1:
            logging.debug("upper_bin is in top bin")
            A1 = min(self.ranges[self.ordered[self.upper_bin]])
            A2 = A1

        logging.debug(
            f"""
            UPPER_BOUND:
            -----
            A1={A1}, A2={A2}, C1={C1}, C2={C2}
            """
        )
        return MedianMoe.get_bound(p=self.p_upper, A1=A1, A2=A2, C1=C1, C2=C2)

    def __call__(self):
        if self.md >= list(self.ranges.values())[-1][0]:
            moe = np.nan
        elif self.B == 0:
            moe = np.nan
        elif self.se_50 >= 50:
            moe = np.nan
        elif self.lower_bin >= len(self.ordered) - 1:
            moe = np.nan
        else:
            moe = (self.upper_bound - self.lower_bound) * 1.645 / 2

        logging.debug(
            f"""
            =================================
            STATS:
            -----
            Median = {self.md}
            Median_MOE = {moe}
            B = {self.B}
            se_50 = {self.se_50}
            p_lower = {self.p_lower}
            p_upper = {self.p_upper}
            lower_bin = {self.lower_bin}
            upper_bin = {self.upper_bin}
            first_non_zero_bin = {self.first_non_zero_bin}

            DISTRIBUTION:
            -----
            {self.cumm_dist_given_bin}
            =================================
            """
        )

        return moe
