from factfinder.download import Download
from factfinder.metadata import Variable

from . import api_key

d = Download(api_key, year=2019, source="acs", geography=2010)


def test_download():
    # geotypes = ['city', 'borough', 'tract']
    # pff_variables = ['lgarab2', 'mpop15t19', 'f16pl']
    df = d("city", "f16pl")
    print(df.head())
