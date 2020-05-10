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
    year: str
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

    def __init__(self, year='2018'):
        self.year = str(year)
        self.state_fips, self.county_fips = self._get_fips(f'{self.year}')

    def _get_fips(self, year):
        """Return FIP code dicts for geoids from self.year of the census"""
        fips_df = pd.read_excel(
            f'https://www2.census.gov/programs-surveys/popest/geographies/{year}/all-geocodes-v{year}.xlsx',
            header=4, dtype=str)

        state_fips_dict = self._get_state_fips_dict(fips_df)
        county_fips_dict = self._get_county_fips_dict(fips_df)

        return state_fips_dict, county_fips_dict

    def _get_state_fips_dict(self, fips_df):
        """Return Dictionary with keys='state', values='FIP code'"""

        state_mask = fips_df['Summary Level'] == '040'
        state_cols = {'state_fip': 'State Code (FIPS)',
                      'state_name':
                          'Area Name (including legal/statistical area description)'}

        state_fips_df = fips_df[state_mask][state_cols.values()].set_index(state_cols['state_name'])

        return state_fips_df.to_dict()[state_cols[0]]

    def _get_county_fips_dict(self, fips_df):
        """Return dict with keys = ('state', 'county_name'), values='FIP code'"""
        county_mask = fips_df['Summary Level'] == '050'
        county_cols = {'county_fip': 'County Code (FIPS)',
                       'state_fip': 'State Code (FIPS)',
                       'county_name':
                           'Area Name (including legal/statistical area description)'}

        county_fips_df = fips_df[county_mask][county_cols.values()]
        county_fips_df = county_fips_df.set_index([county_cols['state'],
                                                   county_cols['county_name']])

        return county_fips_df.to_dict()[county_cols['county_fip']]


class CensusBoundaries(CensusAPI):
    """
    API wrapper for retrieving census boundary files

    Attributes
    ----------
    year: str
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

    def __init__(self, year='2018'):
        super().__init__(year)

        self.base_url = f'https://www2.census.gov/geo/tiger/TIGER{self.year}/'
        self.filepath_dict = {'county': {'directory': 'COUNTY/',
                                         'filename':
                                         lambda state_fip: f'tl_{self.year}_us_county.zip'},
                              'cousub': {'directory': 'COUSUB/',
                                         'filename':
                                         lambda state_fip: f'tl_{self.year}_{state_fip}_cousub.zip'},
                              'tract': {'directory': 'TRACT/',
                                        'filename':
                                        lambda state_fip: f'tl_{self.year}_{state_fip}_tract.zip'},
                              'ttract': {'directory': 'TTRACT/',
                                         'filename':
                                         lambda state_fip: f'tl_{self.year}_us_ttract.zip'},
                              'bg': {'directory': 'BG/',
                                     'filename':
                                     lambda state_fip: f'tl_{self.year}_{state_fip}_bg.zip'},
                              'block': {'directory': 'TABBLOCK/',
                                        'filename':
                                        lambda state_fip: f'tl_{self.year}_{state_fip}_tabblock10.zip'}}

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

        print('Converting to gdf...')

        i = 0
        while i < len(boundary_shp_files):
            try:
                shp_file = boundary_shp_files[i]
                gdf = gpd.read_file(shp_file)
                return gdf
            except:
                if i == len(boundary_shp_files):
                    raise ValueError('There was an error converting the shp file')
                continue

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
        local_path = '/tmp/'
                
        zip_file = self._unzip_file(file_path, local_path)
        print('Finding .shp files...')
        shp_files = [file for file in zip_file.namelist() if file.endswith('.shp')] 

        return [os.path.join(local_path, file) for file in shp_files]
    
    def _get_filepath(self, state_fip, level):
        
        directory = self.filepath_dict[level]['directory']
        filepath = self.filepath_dict[level]['filename'](state_fip)

        return os.path.join(self.base_url, directory, filepath)

    def _unzip_file(self, file_path, local_path):

        url = file_path
        print(f'Downloading {file_path}...')
        r = requests.get(url)
        print(f'To Zip file...')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print(f'Unzipping file...')
        z.extractall(path=local_path)

        return z

class CensusDataAPI(CensusAPI):

    def __init__(self, survey, year):
        super().__init__(year=year)
        self.survey = survey



if __name__ == '__main__':

    census_api = CensusAPI('2018')
    co_fip_num = census_api.state_fips['Colorado']
    jeffco_fip_num = census_api.county_fips[(co_fip_num, 'Jefferson County')]
    print(f'CO FIP: {co_fip_num}, JeffCo FIP: {jeffco_fip_num}')

    census_bondaries = CensusBoundaries('2018')
    county_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'county')
    block_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'block')
    print(county_gdf.head())
    print(block_gdf.head())
