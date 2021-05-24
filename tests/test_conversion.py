from factfinder.calculate import Calculate
from factfinder.metadata import Metadata

from . import api_key

calculate = Calculate(api_key=api_key, year=2019, source="acs", geography="2010_to_2020")

def test_calculate_e_m():
    pff_variable = "pop_1"
    geography = ["CT20", "NTA", "CDTA", "city"]
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


def test_calculate_e_m_median():
    pff_variable = "mdage"
    df = calculate.calculate_e_m_median(pff_variable, geotype="NTA")
    print("\n")
    print(df.head())
    assert True


def test_calculate_e_m_special():
    pff_variable = "mnhhinc"
    df = calculate.calculate_e_m_special(pff_variable, geotype="NTA")
    print("\n")
    print(df.head())
    assert True


def test_calculate_c_e_m_p_z():
    df = calculate.calculate_c_e_m_p_z("mdage", "NTA")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("pop_1", "city")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("mnhhinc", "city")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("mnhhinc", "NTA")
    print("\n")
    print(df.head())
    df = calculate.calculate_c_e_m_p_z("mnhhinc", "CDTA")
    print("\n")
    print(df.head())

def test_calculate_normal():
    for var in calculate.meta.normal_variables:
        df = calculate(var, "CT20")
        print(f"\n{var} CT20")
        print(df.head())

def test_calculate_base():
    for var in calculate.meta.base_variables:
        df = calculate(var, "CT20")
        print(f"\n{var} CT20")
        print(df.head())

def test_calculate_median():
    for var in calculate.meta.median_variables:
        df = calculate(var, "CT20")
        print(f"\n{var} CT20")
        print(df.head())

def test_calculate_special():
    for var in calculate.meta.special_variables:
        df = calculate(var, "CT20")
        print(f"\n{var} CT20")
        print(df.head())
    
def test_calculate_profile_only():
    for var in calculate.meta.profile_only_variables:
        df = calculate(var, "CT20")
        print(f"\n{var} CT20")
        print(df.head())
