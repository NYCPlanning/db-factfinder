import importlib


def test_geography_2010():
    AggregatedGeography = importlib.import_module(
        "factfinder.geography.2010"
    ).AggregatedGeography
    geography = AggregatedGeography()
    for geo in ["cd", "NTA", "cd_fp_100", "cd_park_access", "cd_fp_500"]:
        assert geo in geography.aggregated_geography
