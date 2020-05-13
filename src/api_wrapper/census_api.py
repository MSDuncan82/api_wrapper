"""
API wrappers for interacting with census data.

Usage:

    from api_wrapper.api import CensusBoundaries

    census_bondaries = CensusBoundaries('2018')
    county_gdf = census_bondaries.get_boundaries_gdf('Ignored', 'county')
    block_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'block')
"""

import pandas as pd
import geopandas as gpd
import os
import censusdata
import requests
import io
import zipfile


class CensusAPI(object):
    """
    A base class to interact with census apis

    Attributes
    ----------
    year: int
        The year of census data to be accessed.
    state_fips: dict
        Dictionary with keys='state', values='FIP code'
        Example: {'Colorado':'08'}
    county_fips: dict
        Dictionary with keys=('state', 'county'), values='FIP code'
        Example: {('Colorado', 'Jefferson County', :'059'}

    Methods
    -------
    None

    """

    def __init__(self, year=2018):
        """Initiate CensusAPI object for a given year"""

        self.year = int(year)
        self.state_fips, self.county_fips = self._get_fips(f"{self.year}")

        self.table_list = pd.read_excel(
            "https://www2.census.gov/programs-surveys/acs/tech_docs/table_shells/table_lists/2018_DataProductList.xlsx?#"
        )

    def _get_fips(self, year):
        """Return FIP code dicts for geoids from self.year of the census"""

        fips_df = pd.read_excel(
            f"https://www2.census.gov/programs-surveys/popest/geographies/{year}/all-geocodes-v{year}.xlsx",
            header=4,
            dtype=str,
        )

        state_fips_dict = self._get_state_fips_dict(fips_df)
        county_fips_dict = self._get_county_fips_dict(fips_df)

        return state_fips_dict, county_fips_dict

    def _get_state_fips_dict(self, fips_df):
        """Return Dictionary with keys='state', values='FIP code'"""

        state_mask = fips_df["Summary Level"] == "040"
        state_cols = {
            "state_fip": "State Code (FIPS)",
            "state_name": "Area Name (including legal/statistical area description)",
        }

        state_fips_df = fips_df[state_mask][state_cols.values()].set_index(
            state_cols["state_name"]
        )

        state_fips_dict = state_fips_df.to_dict()[state_cols["state_fip"]]
        state_fips_dict.update({fip: fip for fip in state_fips_dict.values()})

        return state_fips_dict

    def _get_county_fips_dict(self, fips_df):
        """Return dict with keys = ('state', 'county_name'), values='FIP code'"""

        county_mask = fips_df["Summary Level"] == "050"
        county_cols = {
            "county_fip": "County Code (FIPS)",
            "state_fip": "State Code (FIPS)",
            "county_name": "Area Name (including legal/statistical area description)",
        }

        county_fips_df = fips_df[county_mask][county_cols.values()]
        county_fips_df = county_fips_df.set_index(
            [county_cols["state_fip"], county_cols["county_name"]]
        )

        county_fips_dict = county_fips_df.to_dict()[county_cols["county_fip"]]
        county_fips_dict.update({fip: fip for fip in county_fips_dict.values()})

        return county_fips_dict


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

    def __init__(self, year=2018):
        """Initiate CensusBoundaries object for a given year"""

        super().__init__(year)

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


class CensusDataAPI(CensusAPI):
    """
    API wrapper for retrieving data from the census website.

    Wraps another API from PyPI `censusdata` to create a simpler interface.
    The wrapper does take some functionality away from API but it makes it much simpler
    to get useful information. See the underlying API docs for details:
    https://jtleider.github.io/censusdata/

    Attributes
    ----------
    year: int
        The year of census data to be accessed.
    survey: str
        The survey from the census that you want to access information from.
        Ex: 'acs5', 'acs3', acs1', 'acsse', 'sf1'
    tables_dict: dict
        Dictionary of str representions of census tables

    Methods
    ---------
    get_data(tables, state, level)
        method description
    """

    def __init__(self, survey, year):
        """Initiate CensusDataAPI object for a specific `survey` and `year`"""

        super().__init__(year=year)
        self.survey = survey

        # self.tables_dict = {
            # "pop": {"values": ["B01003_001E"], "moe": ["B01003_001M"]},
            # "HI": {
                # "values": [f"B19001_0{table_num:02}E" for table_num in range(1, 17)],
                # "moe": [f"B19001_0{table_num:02}M" for table_num in range(1, 17)],
            # },
            # "med_HI": {"values": ["B19013_001E"], "moe": ["B19013_001M"]},
            # "agg_HI": {"values": ["B19025_001E"], "moe": ["B19025_001M"]},
            # "age": {
                # "values": [f"B01001_0{table_num:02}E" for table_num in range(1, 49)],
                # "moe": [f"B01001_0{table_num:02}M" for table_num in range(1, 49)],
            # },
        # }

        self.tables_dict = {'pop':'B01003', 'HI':'B19001', 'med_HI':'B19013', 'agg_HI':'B19025'}

        hierarchies_csv = "../../data/geo_hierarchies.csv"
        self.hierarchies_dict = self._get_hierarchies(hierarchies_csv)

    def get_data(self, tables=None, **kwargs):
        """
        Get data from survey and year of class for given tables and geoids
        
        Access census data for given year and survey from class. Only data for 
        given state and level.
        TODO may need to download less data if block level gets too unwieldy.
        
        Parameters
        ----------
        tables : list
            List of tables to be requested. Some string representations are supported.
        
        Returns
        ---------
        df : DataFrame
            Census data in the form of a dataframe.
        """

        if tables is None:
            tables = ["pop", "age", "med_HI"]

        table_ids = self._get_table_ids(tables)

        df = self._get_acs_dfs(table_ids, **kwargs)

        #FIXME self._get_table_descrtiption is half finished but gotta climb
        df.columns = [self._get_table_descrption(col) for col in df.columns]

        return df

    def _get_table_ids(self, tables):
        """Return table ids from list of tables ids and strings"""

        table_list_of_lists = [self._parse_table_str(table_str) for table_str in tables]

        # flatten lists of lists
        table_ids = [
            table for table_list in table_list_of_lists for table in table_list
        ]

        return table_ids

    def _parse_table_str(self, table_str):
        """Return table_id from table str"""

        base_table_id = self.tables_dict.get(table_str)

        if base_table_id is None:
            return [base_table_id]

        tables_dict = censusdata.censustable(self.survey, self.year, base_table_id) 

        final_table_dict = {}
        for table_id, table_dict in tables_dict.items():

            table_label = table_dict['concept'] + '!!' + table_dict['label']
            table_id_moe = table_id.replace('E', 'M')
            table_label_moe = "MOE!!" + table_label

            final_table_dict.update({table_id:table_label, table_id_moe:table_label_moe})

        return final_table_dict

    def _get_acs_dfs(self, tables, **kwargs):
        """Get American Community Survey data"""

        hierarchy = self._parse_hierarchy(kwargs)

        df = censusdata.download(
            self.survey, self.year, censusdata.censusgeo(hierarchy), tables,
        )

        return df

    def _parse_hierarchy(self, kwargs):
        """Parse **kwargs (i.e. state='Colorado', county='Jefferson County')
        into [('state', '08'), ('county', '059')]"""

        kwargs["state"] = self.state_fips[kwargs["state"]]
        if "county" in kwargs:
            kwargs["county"] = self.county_fips[(kwargs["state"], kwargs["county"])]

        for level_key, hierarchy_list in self.hierarchies_dict.items():

            are_all_kwargs_in_list = all(
                [level in kwargs.keys() for level in hierarchy_list]
            )
            is_hierachy_correct_length = len(kwargs) == len(hierarchy_list)

            if are_all_kwargs_in_list and is_hierachy_correct_length:
                break

        hierarchy_fips = [(level, kwargs[level]) for level in hierarchy_list]

        hierarchy_fips = self._rename_levels(hierarchy_fips)

        return hierarchy_fips

    def _rename_levels(self, lst):

        levels_rename_dict = {'census_tract':'tract', 'block_group':'block group'}

        out_lst = []
        for level_key, fip in lst:
            if level_key in levels_rename_dict:
                level_key = levels_rename_dict[level_key]

            out_lst.append((level_key, fip))

        return out_lst

    # TODO Actually make human readable
    def _get_table_descrption(self, table):
        """Get human readable table name"""
        breakpoint()
        table_labels_dict = censusdata.censustable(self.survey, self.year, table)
        table_label_dict = table_labels_dict[table]

        return table_label_dict["label"], table_label_dict["concept"]

    def _get_hierarchies(self, csv):

        hierarchies_df = pd.read_csv(csv)

        hierarchies_list = hierarchies_df.name.str.split("-")
        hierarchies_list = map(
            lambda lst: [string.lower() for string in lst], hierarchies_list
        )

        hierarchies_dict = {
            hierarchy_list[-1]: hierarchy_list for hierarchy_list in hierarchies_list
        }
        hierarchies_dict["block"] = hierarchies_dict["block group"] + ["block"]

        hierarchies_dict = {
            level_key.replace(" ", "_"): [level.replace(" ", "_") for level in levels]
            for level_key, levels in hierarchies_dict.items()
        }

        return hierarchies_dict


if __name__ == "__main__":

    # census_api = CensusAPI("2018")
    # co_fip_num = census_api.state_fips["Colorado"]
    # jeffco_fip_num = census_api.county_fips[(co_fip_num, "Jefferson County")]
    # print(f"CO FIP: {co_fip_num}, JeffCo FIP: {jeffco_fip_num}")

    # census_bondaries = CensusBoundaries("2018")
    # county_gdf = census_bondaries.get_boundaries_gdf("Colorado", "county")
    # block_gdf = census_bondaries.get_boundaries_gdf("Colorado", "block")
    # print(county_gdf.head())
    # print(block_gdf.head())

    census_data = CensusDataAPI("acs5", 2018)
    co_data = census_data.get_data(
        state="Colorado", county="Jefferson County", census_tract="011724", block_group="*"
    )
    print(co_data.head())
