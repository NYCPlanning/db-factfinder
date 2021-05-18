from factfinder.calculate import Calculate

from . import api_key

calculate = Calculate(api_key=api_key, year=2019, source="acs", geography=2010)


def test_calculate_e_m():
    pff_variable = "pop_1"
    geography = ["NTA", "city"]
    for g in geography:
        df = calculate.calculate_e_m(pff_variable, g)
        print("\n")
        print(df.head())
    assert True


def test_calculate_e_m_p_z():
    pff_variable = "f16pl"
    geography = ["tract", "city"]
    for g in geography:
        df = calculate.calculate_e_m_p_z(pff_variable, g)
        print("\n")
        print(df.head())
    assert True


def test_calculate_e_m_multiprocessing():
    pff_variables = ["pop_1", "f16pl", "mdpop10t14"]
    df = calculate.calculate_e_m_multiprocessing(pff_variables, geotype="borough")
    assert df.shape[0] == 15
