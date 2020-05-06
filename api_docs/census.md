# CensusBoundaries

```python
from api_wrapper.api import CensusBoundaries

census_bondaries = CensusBoundaries('2018')
county_gdf = census_bondaries.get_boundaries_gdf('Ignored', 'county')
block_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'block')
```