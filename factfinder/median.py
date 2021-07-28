import logging

import numpy as np


class Median:
    def __init__(self, ranges, row, DF=1.1):
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
        self.row = row

    @property
    def median(self):
        N = self.B
        C = 0
        i = 0
        while C < N / 2 and i <= len(self.ranges.keys()) - 1:
            # Calculate cumulative frequency until half of all units are accounted for
            C += self.row[self.ordered[i]]
            i += 1
        i = i - 1
        if i == 0:
            logging.debug("N/2 is in bottom bin")
            median = list(self.ranges.values())[0][1]
        elif C == 0.0:
            logging.debug("Cumulative frequency is 0")
            median = 0.0
        elif i == len(self.ranges.keys()) - 1:
            logging.debug("N/2 is in top range")
            median = list(self.ranges.values())[-1][0]
        else:
            logging.debug(f"N/2 is in range {list(self.ranges.values())[i]}")
            C = C - self.row[self.ordered[i]]
            L = self.ranges[self.ordered[i]][0]
            F = self.row[self.ordered[i]]
            W = self.ranges[self.ordered[i]][1] - self.ranges[self.ordered[i]][0]
            logging.debug(
                "\n================================="
                f"\nC_{i-1}: Cumulative frequency up to bin below N/2: {C}"
                f"\nL_{i}: Lower boundary of median group: {L}"
                f"\nF_{i}: Frequency within median group: {F}"
                f"\nW_{i}: Width of median group: {W}"
                "\n================================="
            )
            median = L + (N / 2 - C) * W / F
        logging.debug(f"\nMEDIAN: {median}\n")
        return median

    @property
    def cumm_dist_given_bin(self):
        return "\n".join(
            [
                f"- {_bin}: {dist}"
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

        return Median.get_bound(p=self.p_lower, A1=A1, A2=A2, C1=C1, C2=C2)

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
        return Median.get_bound(p=self.p_upper, A1=A1, A2=A2, C1=C1, C2=C2)

    @property
    def median_moe(self):
        if self.median >= list(self.ranges.values())[-1][0]:
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
            "\n================================="
            "\nMEDIAN STATS:"
            "\n-----"
            f"\nMedian = {self.median}"
            f"\nMedian_MOE = {moe}"
            f"\nB = {self.B}"
            f"\nse_50 = {self.se_50}"
            f"\np_lower = {self.p_lower}"
            f"\np_upper = {self.p_upper}"
            f"\nlower_bin = {self.lower_bin}"
            f"\nupper_bin = {self.upper_bin}"
            f"\nfirst_non_zero_bin = {self.first_non_zero_bin}"
            "\n"
            "\nDISTRIBUTION:"
            "\n-----"
            f"\n{self.cumm_dist_given_bin}"
            f"\n================================="
        )

        return moe