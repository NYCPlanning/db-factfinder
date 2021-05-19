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
    for geo in ["CDTA", "NTA"]:
        assert geo in geography.aggregated_geography


def test_2010_to_2020_ct2010_to_ct2020():
    assert True


def test_2010_to_2020_tract_to_nta():
    assert True


def test_2010_to_2020_tract_to_cdta():
    assert True
