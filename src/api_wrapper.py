import requests

class API():
'''A simple wrapper to interact with web apis

Usage: 

mapbox_api= API('https://api.mapbox.com/isochrone/v1/mapbox')
options = {'/driving':None,
           '%2C':[lng, lat],
           '?contours_minutes':'%2c'.join(['5', '10', '15', '20]),
           'polygons':'true',
           'access_token':key}

'''
    def __init__(self, base_url):

        self.base_url = base_url.strip('/')

    def build_request_str(self, options):
        
        options_list = []
        for k, v in options.items():
            
            if v == None:
                options_list.append(k)
            
            elif isinstance(v, str):
                options_list.append(f'{k}={v}')

            elif isinstance(v, dict):
                options_list.append(self.build_request_str(v))
            
            else:
                options_list.append(k.join(v))
            
        request_str = self.base_url + ''.join(options_list)

        print(request_str)
        return request_str

    def get_response(self, request_str):

        response = requests.get(request_str)

        return response

    def get_json(self, get_object):

        if isinstance(get_object, str):

            response = self.get_response(get_object)

        else:

            response = get_object

        return response.json




