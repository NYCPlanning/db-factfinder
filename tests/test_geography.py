import importlib


def test_2010_aggregated_geography():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010"
    ).AggregatedGeography
    geography = AggregatedGeography()
    for geo in ["cd", "NTA", "cd_fp_100", "cd_park_access", "cd_fp_500"]:
        assert geo in geography.aggregated_geography


def test_2010_to_2020_aggregated_geography():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    print(geography.ratio.head())
    print(geography.lookup_geo.head())
    for geo in ["CDTA", "NTA", "CT20"]:
        assert geo in geography.aggregated_geography


def test_2010_to_2020_ct2010_to_ct2020():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    test_data = {'row_1': ["36005001600", 5825, 398], 'row_2': ["36005001900", 3141, 341]}
    test_df = pd.DataFrame.from_dict(test_data, orient='index',
                       columns=['census_geoid', 'e', 'm'])
    df = geography.ct2010_to_ct2020(test_df)
    assert df


def test_2010_to_2020_tract_to_nta():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    test_data = {'row_1': ["36005001600", 5825, 398], 'row_2': ["36005001900", 3141, 341]}
    test_df = pd.DataFrame.from_dict(test_data, orient='index',
                       columns=['census_geoid', 'e', 'm'])
    df = geography.tract_to_nta(test_df)
    assert True


def test_2010_to_2020_tract_to_cdta():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010_to_2020"
    ).AggregatedGeography
    geography = AggregatedGeography()
    test_data = {'row_1': ["36005001600", 5825, 398], 'row_2': ["36005001900", 3141, 341]}
    test_df = pd.DataFrame.from_dict(test_data, orient='index',
                       columns=['census_geoid', 'e', 'm'])
    df = geography.tract_to_cdta(test_df)
    assert True