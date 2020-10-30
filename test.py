from factfinder.main import Pff
import os

pff = Pff(api_key=os.environ['API_KEY'])
print(pff.calculate_variable("pop"))
