# db-factfinder
data ETL for population fact finder (decennial + acs)

# Instructions
1. initialize 
```python 
from factfinder.main import Pff

pff = Pff(
    api_key='XXXXXXXXXXXXXXXXX', 
    year = 2018
)
```
> or for decennial calculations:
```python 
decennial = Pff(
    api_key='XXXXXXXXXXXXXXXXX', 
    year = 2010
)
```
2. Calculate
> Calling `calculate` 
>    1. Downloads necessary census variables,
>    2. Aggregates using appropriate technique (both vertically and horizontally)
>    3. Calculates c, e, m, p, z fields
>    4. Rounds calculated values based on standards in the metadata
>    5. Cleans edge-cases of 0 and null values
```python
df = pff.calculate('pop', 'tract')
df = pff.calculate('pop', 'borough')
df = pff.calculate('pop', 'city')
df = pff.calculate('pop', 'NTA')
df = pff.calculate('mdage', 'NTA')
```

