# Mapbox

```python
from api_wrapper.api import MapboxAPI

key = os.environ['MAPBOX_ISO_APIKEY']

mapbox_api= MapboxAPI(key)

options = dict(travel_type = 'driving',
                lng = '-105.2',
                lat = '39.7',
                contours_minutes=['10', '20', '30'],
                polygons = 'true')

request_str = mapbox_api.get_iso_request_str(**options)

mapbox_api.iso_to_geojson('../../test_data/test.geojson', **options)
```