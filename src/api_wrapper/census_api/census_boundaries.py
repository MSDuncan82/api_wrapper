from api_wrapper.census_api.census_api import CensusAPI
import geopandas as gpd
import os
import requests
import io
import zipfile


class CensusBoundaries(CensusAPI):
    """
    API wrapper for retrieving census boundary files

    Attributes
    ----------
    year: int
        The year of census data to be accessed.
    base_url: str
        base_url for api requests.
    filepath_dict: str
        dictionary of file names and directories associated with the Tiger Line file structure.
        https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html

    Methods
    ---------
    get_boundaries_gdf(self, state, level)
        Get a Geopandas GeoDataFrame of the requested boundary file.
    download_shp(self, state_fip, level)
        download shape files associated with a specific state's FIP code and level.
        The state_fip code is ignored if the level is 'county' or 'ttract'.
    """

    def __init__(self, year=2018, **kwargs):
        """Initiate CensusBoundaries object for a given year"""

        super().__init__(year=year, **kwargs)

        self.base_url = f"https://www2.census.gov/geo/tiger/TIGER{self.year}/"
        self.filepath_dict = {
            "county": {
                "directory": "COUNTY/",
                "filename": lambda state_fip: f"tl_{self.year}_us_county.zip",
            },
            "cousub": {
                "directory": "COUSUB/",
                "filename": lambda state_fip: f"tl_{self.year}_{state_fip}_cousub.zip",
            },
            "tract": {
                "directory": "TRACT/",
                "filename": lambda state_fip: f"tl_{self.year}_{state_fip}_tract.zip",
            },
            "ttract": {
                "directory": "TTRACT/",
                "filename": lambda state_fip: f"tl_{self.year}_us_ttract.zip",
            },
            "bg": {
                "directory": "BG/",
                "filename": lambda state_fip: f"tl_{self.year}_{state_fip}_bg.zip",
            },
            "block": {
                "directory": "TABBLOCK/",
                "filename": lambda state_fip: f"tl_{self.year}_{state_fip}_tabblock10.zip",
            },
        }

    def get_boundaries_gdf(self, state, level):
        """
        Get a Geopandas GeoDataFrame of the requested boundary file.

        Parameters
        ----------
        state: str
            State as string capitalized Ex: 'Alabama', 'Colorado'.
            TODO Allow more flexibility in user input
        level: str
            The level desired of the boundary shape file Ex: 'block', 'county', 'tract'
            TODO Allow more flexibility in user input

        Returns
        ---------
        gdf: GeoDataFrame
            Geopandas GeoDataFrame of boundary shp file downloaded from the census Tiger Line
            shape files.
        """

        state_fip = self.state_fips[state]

        boundary_shp_files = self.download_shp(state_fip, level)

        print("Converting to gdf...")

        i = 0
        while i < len(boundary_shp_files):
            shp_file = boundary_shp_files[i]
            gdf = gpd.read_file(shp_file)
            return gdf

    def download_shp(self, state_fip, level):
        """
        Download shape files associated with a specific state's FIP code and level.

        The state_fip code is ignored if the level is 'county' or 'ttract'.

        Parameters
        ----------
        state_fip: str
            State FIP code as a str '08'
            TODO Allow more flexibility in user input
        level: str
            The level desired of the boundary shape file Ex: 'block', 'county', 'tract'
            TODO Allow more flexibility in user input

        Returns
        ---------
        shp_files: list
            List of shp files in zip file from given year, state and summary level.
        """

        file_path = self._get_filepath(state_fip, level)
        local_path = "/tmp/"

        zip_file = self._unzip_file(file_path, local_path)
        print("Finding .shp files...")
        shp_files = [file for file in zip_file.namelist() if file.endswith(".shp")]

        return [os.path.join(local_path, file) for file in shp_files]

    def _get_filepath(self, state_fip, level):
        """Return the filepath to the requested boundary file"""

        directory = self.filepath_dict[level]["directory"]
        filepath = self.filepath_dict[level]["filename"](state_fip)

        return os.path.join(self.base_url, directory, filepath)

    def _unzip_file(self, file_path, local_path):
        """Unzip a zipfile's contents to `local_path`"""

        url = file_path
        print(f"Downloading {file_path}...")
        r = requests.get(url)
        print(f"To Zip file...")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print(f"Unzipping file...")
        z.extractall(path=local_path)

        return z


if __name__ == "__main__":

    census_boundaries = CensusBoundaries()
