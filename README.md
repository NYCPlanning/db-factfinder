# db-factfinder
data ETL for population fact finder (decennial + acs)

# Instructions
1. initialize 
```python 
from factfinder.main import Pff

pff = Pff(
    api_key='XXXXXXXXXXXXXXXXX', 
)
```
2. Calculate
> for none median variables: 
```python
df = pff.calculate_variable('pop', 'tract')
df = pff.calculate_variable('pop', 'borough')
df = pff.calculate_variable('pop', 'city')
df = pff.calculate_variable('pop', 'NTA')

```
> for median variables: 
```python
df = pff.calculate_median_variable('mdage', 'NTA')
```