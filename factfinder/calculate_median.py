# class CalculateMedian:
#     def __init__(self) -> None:
#         self.median_ranges =
#         self.


def calculate_median_e_m(pff_variable, geotype) -> pd.DataFrame:
    """
    Given median variable in the form of pff_variable and geotype
    calculate the median and median moe
    """
    # 1. Initialize
    ranges = self.median_ranges(pff_variable)
    design_factor = self.median_design_factor(pff_variable)
    rounding = self.create_variable(pff_variable).rounding

    # 2. Calculate each variable that goes into median calculation
    df = self.calculate_multiple_e_m(list(ranges.keys()), geotype)

    # 3. create a pivot table with census_geoid as the index, and
    # pff_variable as column names. df_pivoted.e -> the estimation dataframe
    df_pivoted = df.loc[:, ["census_geoid", "pff_variable", "e"]].pivot(
        index="census_geoid", columns="pff_variable", values=["e"]
    )

    # Empty dataframe to store the results
    results = pd.DataFrame()
    results["census_geoid"] = df_pivoted.index
    results["pff_variable"] = pff_variable
    results["geotype"] = geotype

    # 4. calculate median estimation using get_median
    results["e"] = (
        df_pivoted.e.loc[
            df_pivoted.e.index == results.census_geoid, list(ranges.keys())
        ]
        .apply(lambda row: get_median(ranges, row), axis=1)
        .to_list()
    )

    # 5. Calculate median moe using get_median_moe
    # Note that median moe calculation needs the median estimation
    # so we seperated df_pivoted.m out as a seperate dataframe
    e = df_pivoted.e.copy()
    e["e"] = results.loc[e.index == results.census_geoid, "e"].to_list()
    results["m"] = (
        e.loc[e.index == results.census_geoid, list(ranges.keys()) + ["e"]]
        .apply(lambda row: get_median_moe(ranges, row, design_factor), axis=1)
        .to_list()
    )

    # 6. return the output, containing the median, and all the variables used
    return results
