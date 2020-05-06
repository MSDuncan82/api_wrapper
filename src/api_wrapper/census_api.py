import pandas as pd
import geopandas as gpd
import dotenv
import os
from base_api import API
import censusdata
import requests
import io
import zipfile

class CensusAPI(object):
    """A wrapper to interact with census apis

    CensusAPI is a wrapper around the PyPI package censusdata

    Usage: 

        census_api = CensusAPI('2018')

        co_fip_num = census_api.state_fips['Colorado']
        jeffco_fip_num = census_api.county_fips[(co_fip_num, 'Jefferson County')]

        print(f'CO FIP: {co_fip_num}, JeffCo FIP: {jeffco_fip_num}')
    """
    
    def __init__(self, year='2018'):
        
        self.year = year
        self.state_fips, self.county_fips = self.get_fips(f'{self.year}')

    def get_fips(self, year):

        fips_df = pd.read_excel(
            'https://www2.census.gov/programs-surveys/popest/geographies/2018/all-geocodes-v2018.xlsx',
            header=4, dtype=str)

        state_fips_dict = self.get_state_fips_dict(fips_df)
        county_fips_dict = self.get_county_fips_dict(fips_df)

        return state_fips_dict, county_fips_dict

    def get_state_fips_dict(self, fips_df):

        state_mask = fips_df['Summary Level'] == '040'
        state_cols = ['State Code (FIPS)',
                      'Area Name (including legal/statistical area description)']

        state_fips_df = fips_df[state_mask][state_cols].set_index(state_cols[1])

        return state_fips_df.to_dict()[state_cols[0]]

    def get_county_fips_dict(self, fips_df):
        
        county_mask = fips_df['Summary Level'] == '050'
        county_cols = ['County Code (FIPS)',
                    'State Code (FIPS)',
                    'Area Name (including legal/statistical area description)']

        county_fips_df = fips_df[county_mask][county_cols].set_index([county_cols[-2], county_cols[-1]])

        return county_fips_df.to_dict()[county_cols[0]]

class CensusBoundaries(CensusAPI):
    """API to download boundaries from Tiger Line

    CensusBoundaries creates Geopandas GeoDataframes from shp files
    from Census.gov Tiger Line

    Usage:

        census_bondaries = CensusBoundaries('2018')
        county_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'county')
        block_gdf = census_bondaries.get_boundaries_gdf('Colorado', 'block')
    """

    def __init__(self, year='2018'):
        super().__init__(year)

        self.base_url = f'https://www2.census.gov/geo/tiger/TIGER{self.year}/'
        self.filepath_dict = {'county':{'directory':'COUNTY/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_us_county.zip'},
                              'cousub':{'directory':'COUSUB/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_{state_fip}_cousub.zip'},
                              'tract':{'directory':'TRACT/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_{state_fip}_tract.zip'},
                              'ttract':{'directory':'TTRACT/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_us_ttract.zip'},
                              'bg':{'directory':'BG/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_{state_fip}_bg.zip'},
                              'block':{'directory':'TABBLOCK/',
                                        'filename':
                                        lambda state_fip:f'tl_{self.year}_{state_fip}_tabblock10.zip'}} 

    def get_boundaries_gdf(self, state, level):

        state_fip = self.state_fips[state]

        boundary_shp_files = self.download_shp(state_fip, level)

        print('Converting to gdf...')

        i = 0
        while i<len(boundary_shp_files):
            try:
                shp_file = boundary_shp_files[i]
                gdf = gpd.read_file(shp_file)
                return gdf
            except:
                if i == len(boundary_shp_files):
                    raise ValueError('There was an error converting the shp file')
                continue

    def download_shp(self, state_fip, level):

        file_path = self.get_filepath(state_fip, level) 
        local_path = '/tmp/'
                
        zip_file = self.unzip_file(file_path, local_path)
        print('Finding .shp files...')
        shp_files = [file for file in zip_file.namelist() if file.endswith('.shp')] 

        return [os.path.join(local_path, file) for file in shp_files]
    
    def get_filepath(self, state_fip, level):
        
        directory = self.filepath_dict[level]['directory']
        filepath = self.filepath_dict[level]['filename'](state_fip)

        return os.path.join(self.base_url, directory, filepath)

    def unzip_file(self, file_path, local_path):

        url = file_path
        print(f'Downloading {file_path}...')
        r = requests.get(url)
        print(f'To Zip file...')
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print(f'Unzipping file...')
        z.extractall(path=local_path)

        return z

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