import requests
import dotenv
import os
from api_wrapper.base_api import API
import geopandas as gpd


class GeoAPI(API):
    """
    A simple wrapper to interact with geocoding web apis
    
    GeoAPI is a class built on top of the base API with geo features.

    Attributes
    ----------
    base_url : str
        The web API's base url.
    
    Methods
    ---------
    request_to_geojson(request_str, filepath)
        Return geojson from request str. Respons json must use `features` key.

    """

    def __init__(self, base_url):
        """Initiate GeoAPI instance with base_url of web api"""

        super().__init__(base_url)

    def request_to_geojson(self, request_str, filepath):
        """Return geojson from request str. Response json must use `features` key."""

        json = self.get_json(request_str)
        features = json["features"]

        gdf = gpd.GeoDataFrame.from_features(features)
        gdf.to_file(filepath, driver="GeoJSON")


class MapboxAPI(object):
    """
    Wrapper for Mapbox API.
    
    Attributes
    ----------
    iso_api : GeopAPI Instance
        GeoAPI Object with Mapbox's isochrone web api base_url
    access_token: str
        User's API key

    Methods
    ---------
    get_iso_request_str(lng, lat, travel_type="driving", contours_minutes=("10", "20", "30"), polygons="true")
        Return request string for Mapbox isochrones
    get_iso_json(self, request_str):
        Return json from isochrone request string
    iso_to_geojson(filepath, lng, lat, travel_type="driving", contours_minutes=("10", "20", "30"), polygons="true")
        Return geojson from isochrone request kwargs

    """

    def __init__(self, key):
        """Initiate MapboxAPI object with API key"""

        self.iso_api = GeoAPI("https://api.mapbox.com/isochrone/v1/mapbox")
        self.access_token = key

    def get_iso_request_str(
        self,
        lng,
        lat,
        travel_type="driving",
        contours_minutes=("10", "20", "30"),
        polygons="true",
    ):
        """Return request string for Mapbox isochrones"""

        options = {
            f"/{travel_type}/": None,
            "%2C": [lng, lat],
            "?contours_minutes": "%2c".join(contours_minutes),
            "&polygons": polygons,
            "&access_token": self.access_token,
        }

        request_str = self.iso_api.get_request_str(options)

        return request_str

    def get_iso_json(self, request_str):
        """Return json from isochrone request string"""

        json = self.iso_api.get_json(request_str)

        return json

    def iso_to_geojson(
        self,
        filepath,
        lng,
        lat,
        travel_type="driving",
        contours_minutes=("10", "20", "30"),
        polygons="true",
    ):
        """Return geojson from iso request dictionary"""

        request_str = self.get_iso_request_str(
            lng, lat, travel_type, contours_minutes, polygons
        )

        self.iso_api.request_to_geojson(request_str, filepath)


if __name__ == "__main__":

    dotenv_path = "../../.env"
    dotenv.load_dotenv(dotenv_path)

    key = os.environ["MAPBOX_ISO_APIKEY"]

    mapbox_api = MapboxAPI(key)

    options = dict(
        travel_type="driving",
        lng="-105.15742179",
        lat="39.673203005",
        contours_minutes=["10", "20", "30"],
        polygons="true",
    )

    request_str = mapbox_api.get_iso_request_str(**options)

    mapbox_api.iso_to_geojson("../../test_data/test.geojson", **options)
