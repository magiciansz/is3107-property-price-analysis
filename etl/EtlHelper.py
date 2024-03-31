import json
import datetime
import requests
import pandas as pd
import time
import numpy as np
from RetrieveDB import RetrieveDB

#this function takes in your OneMap API username, password
#returns access token string
class EtlHelper:
    def __init__(self):
        pass
    ###AUTHORIZATION FUNCTIONS
    def one_map_authorise(self, username, password):
      auth_url = "https://www.onemap.gov.sg/api/auth/post/getToken"

      payload = {
              "email": username,
              "password": password
              }

      # response = requests.request("POST", auth_url, json=payload)
      headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
      response = requests.post(auth_url, headers=headers, json=payload)
      # json_data = json.loads(response.text)

      return response.json()['access_token']

    def ura_authorise(self, ura_access_key):
      auth_url = 'https://www.ura.gov.sg/uraDataService/insertNewToken.action'
      headers = {'AccessKey': ura_access_key, 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
      response = requests.post(auth_url, headers=headers)
      return response.json()['Result']

    ###EXTRACT RELATED FUNCTIONS
    #this function takes in one OPTIONAL argument: year, the specified year (as a STRING) to get planning area polygons, if not supplied, gives latest master plan year
    #returns a list of dicts, each dict contain pln area name and geojson string
    def extract_planning_area_polygon(self, ONEMAP_TOKEN, year=""):
      url = "https://www.onemap.gov.sg/api/public/popapi/getAllPlanningarea"
      headers = {
              "Authorization": ONEMAP_TOKEN
              }
      params = {
              "year": year
              }
      response = requests.request("GET", url, headers=headers, params=params)
      json_data = json.loads(response.text)
      return json_data['SearchResults']

    def extract_ura_data(self, batch_no, ura_access_key, ura_access_token):
      url = 'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction'
      headers = {'AccessKey': ura_access_key, 'Token': ura_access_token, 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
      params = {'batch': batch_no}
      data = requests.get(url, headers=headers, params=params)
      return data.json()

    #this function takes in two string, start date and end date, in format of %Y-%m
    #returns a list of months inbetween the two dates (inclusive)
    def get_list_of_year_months(self, start_year_month, end_year_month):
      return pd.date_range(start_year_month,end_year_month, freq='MS').strftime("%Y-%m").tolist()

    #this function takes in a string representing the year-month of interest to be extracted from HDB dataset
    #returns a list of dictionaries, each dictionary is one row of data
    def extract_hdb_data(self, year_month):
      data = {
      "filters": '{"month":"' + year_month + '"}',
      "limit": "10000",
      }
      search_url = 'https://data.gov.sg/api/action/datastore_search?resource_id=d_8b84c4ee58e3cfc0ece0d773c8ca6abc'

      response = requests.request("GET", search_url, params=data)
      #TODO: Check if number of results is at limit (suggests that there may be trruncated values)
      #for now assume each month's data will NOT exceed 10K.

      #attempt to process API response and assign lat and long values
      json_data = json.loads(response.text)
      return json_data['result']['records']

    ###TRANSFORM RELATED FUNCTIONS
    # this function takes in two values, x and y coordinates.
    # it returns two strings: the latitude and longitude corresponding to these x and y coordinates
    def coordinates_to_lat_long(self, x, y, ONEMAP_TOKEN):
      location = x + ',' + y
      url = 'https://www.onemap.gov.sg/api/public/revgeocodexy'
      headers = {"Authorization": ONEMAP_TOKEN, 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
      params = {
          'location': location,
          'buffer': 40,
          'addressType': 'All',
          'otherFeatures': 'N'
      }
      
      try:
        response = requests.get(url, params=params, headers=headers)
        response = response.json()['GeocodeInfo']
      except:
        return ("NA", "NA")
      else:
        try:
          lat, long = response[0]['LATITUDE'], response[0]['LONGITUDE']
        except:
          return ("NA", "NA")
        else:
          return lat, long
    
    #this function takes in two values: lat and long
    ##returns one string: planning area name
    def get_planning_area_name_from_lat_long(self, lat, long, ONEMAP_TOKEN):
      if (lat=="NA" or long=="NA"):
        return "NA"

      planning_query_url = "https://www.onemap.gov.sg/api/public/popapi/getPlanningarea"
      headers = {"Authorization": ONEMAP_TOKEN}
      params = {
              "latitude": lat,
              "longitude": long
              }

      response = requests.request("GET", planning_query_url, params = params, headers=headers)
      #try up to 3 times if API call fails
      for i in range(3):
        try:
          json_data = json.loads(response.text)
        except:
          if i <=1:
            continue
          else:
            return "NA"
        else:
          try: 
            json_data[0]['pln_area_n']
          except KeyError:
            return "NA"
          else:
            return json_data[0]['pln_area_n']
        
    # this function takes in a URA dataset, and assigns the lat and long based on the helper function above.
    def assign_long_lat_to_ura_dataset(self, dataset, ONEMAP_TOKEN):
      function_start = time.time()
      coordinates_mapping = {}
      total_entries = len(dataset)
      for count, property in enumerate(dataset):
        start = time.time()
        print('progress: ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion')
        if 'x' in property and 'y' in property:
          x, y = property['x'], property['y']
          if (x, y) not in coordinates_mapping:
              coordinates_mapping[(x, y)] = self.coordinates_to_lat_long(x, y, ONEMAP_TOKEN)
          property['lat'], property['long'] = coordinates_mapping[(x, y)]
        else:
          property['lat'], property['long'] = "NA", "NA"
        end = time.time()
        print(property['lat'], property['long'])
        print('time taken for ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion is ' + str(end - start))
      function_end = time.time()
      print('time taken for whole function is ' + str(function_end - function_start))
      return dataset

    def assign_planning_area_to_ura_dataset(self, dataset, ONEMAP_TOKEN):
      function_start = time.time()
      coordinates_to_district_mapping = {}
      total_entries = len(dataset)
      for count, property in enumerate(dataset):
        start = time.time()
        print('progress: ' + str(count) + '/' + str(total_entries) + ' for planning area')
        lat, long = property['lat'], property['long']
        if (lat, long) not in coordinates_to_district_mapping:
          coordinates_to_district_mapping[(lat, long)] = self.get_planning_area_name_from_lat_long(property['lat'], property['long'], ONEMAP_TOKEN)
        property['planning_area'] = coordinates_to_district_mapping[(lat, long)]
        end = time.time()
        print(property['planning_area'])
        print('time taken for ' + str(count) + '/' + str(total_entries) + ' for planning area is ' + str(end - start))
      function_end = time.time()
      print('time taken for whole function is ' + str(function_end - function_start))
      return dataset

    #this function takes in a search string (assumes previously concatenated by user),
    ##returns two strings: lat and long values of the property respectively
    def address_to_lat_long(self, search_term):
      data = {
        "searchVal": search_term,
        "returnGeom": "Y",
        "getAddrDetails": "Y"
        }
      search_url = "https://www.onemap.gov.sg/api/common/elastic/search"

      response = requests.request("GET", search_url, params=data)
      #attempt to process API response and assign lat and long values
      #try up to 3 times if API call fails
      for i in range(3):
        try:
          json_data = json.loads(response.text)
          lat = json_data["results"][0]["LATITUDE"]
          long = json_data["results"][0]["LONGITUDE"]
        # if json processing fails (for reasons such as empty json response due to invalid address) return "NA" for lat and long
        except:
          if i <=1:
            continue
          else:
            return "NA", "NA"
        else:
          return lat, long

    # this function takes in a HDB dataset, and assigns the lat and long of first search result based on concatenated address, block and street name columns
    def assign_long_lat_to_hdb_dataset(self, dataset):
      function_start = time.time()
      address_mapping= {}
      total_entries = len(dataset)
      for count, property in enumerate(dataset):
        start = time.time()
        print('progress: ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion')
        address = property["block"] + " " + property["street_name"]
        if address not in address_mapping:
          address_mapping[address] = self.address_to_lat_long(address)
        property['lat'], property['long'] = address_mapping[address]
        end = time.time()
        print(property['lat'], property['long'])
        print('time taken for ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion is ' + str(end - start))
      function_end = time.time()
      print('time taken for whole function is ' + str(function_end - function_start))
      return dataset

    #this function takes in a HDB dataset, and assigns the planning area based on the helper function above
    def assign_planning_area_to_hdb_dataset(self, dataset, ONEMAP_TOKEN):
      function_start = time.time()
      coordinates_to_district_mapping = {}
      total_entries = len(dataset)
      for count, property in enumerate(dataset):
        start = time.time()
        print('progress: ' + str(count) + '/' + str(total_entries) + ' for planning area')
        lat, long = property['lat'], property['long']
        if (lat, long) not in coordinates_to_district_mapping:
          coordinates_to_district_mapping[(lat, long)] = self.get_planning_area_name_from_lat_long(property['lat'], property['long'], ONEMAP_TOKEN)
        property['planning_area'] = coordinates_to_district_mapping[(lat, long)]
        end = time.time()
        print(property['planning_area'])
        print('time taken for ' + str(count) + '/' + str(total_entries) + ' for planning area is ' + str(end - start))
      function_end = time.time()
      print('time taken for whole function is ' + str(function_end - function_start))
      return dataset

    ###LOAD RELATED FUNCTIONS
    # this function takes in a URA dataset, and removes records that are not relevant to the current update pipeline.

    def filter_current_month_dataset(self, dataset):
      current_date = datetime.now()
      formatted_date = current_date.strftime("%m%y")
      filtered_dataset = []
      for project in dataset:
        valid_transactions = list(filter(lambda x: x['contractDate'] == formatted_date, project['transaction']))
        if valid_transactions:
          project['transaction'] = valid_transactions
          filtered_dataset.append(project)
      return filtered_dataset
      
    def _get_projects_helper(self, df):
      # NOT NULL columns
      project_cols = ['project_name', 'district_name', 'long', 'lat', 'address']
      df.drop_duplicates(subset = ['address', 'lat', 'long', 'project_name'], inplace=True)
      df = df.reset_index()[project_cols]
      return df

    def _get_property_helper(self, df):
      property_cols = ['property_type', 'lease_year', 'lease_duration', 'floor_range_start', 'floor_range_end', 'floor_area']
      project_cols = ['project_name', 'long', 'lat', 'address']
      df = df[property_cols + project_cols]
      return df

    def _get_transaction_helper(self, df):
      tx_cols = ['transaction_year', 'transaction_month', 'type_of_sale', 'price']
      df = df[tx_cols]
      return df
    
    def load_district_df(self, district_filepath):
      with open(district_filepath, 'r') as file:
         district_dict = json.load(file)['Result']
      district_dict['coord_list'] = {key: str(val) for key, val in district_dict['coord_list'].items()}
      district_df = pd.DataFrame(district_dict).rename(columns={"pln_area_n": "district_name", "coord_list": "coordinates"})
      return district_df

    # if you see code blurred out by Pylance, it actually still reaches it. Pylance is blurring it by mistake
    def load_hdb_ura_to_project(self, hdb_filepath, ura_filepath):
      dbretrieve = RetrieveDB()
      # cleaned datasets
      hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      # prepare data for Project table
      project_df = pd.concat([self._get_projects_helper(ura), self._get_projects_helper(hdb)]).reset_index(drop=True)
      # replace pd.NA with None for inserting into SQL + comparisons to get district ID
      project_df = project_df.replace({np.nan: None})
      district_name_to_id_mapping = dbretrieve.get_district_name_to_id_mapping()
      project_df['district_id'] = project_df['district_name'].map(district_name_to_id_mapping)
      # drop column used to map district ID
      project_df = project_df.drop('district_name', axis=1)
      return project_df

    def load_hdb_ura_to_property(self, hdb_filepath, ura_filepath):
      dbretrieve = RetrieveDB()
      hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      property_df = pd.concat([self._get_property_helper(ura), self._get_property_helper(hdb)]).reset_index(drop=True)
      # replace pd.NA with None for inserting into SQL + comparisons to get project ID
      property_df = property_df.replace({np.nan: None})
      project_details_to_id_mapping = dbretrieve.get_project_details_to_id_mapping()
      property_df['project_id'] = property_df.apply(
        lambda x: project_details_to_id_mapping.get((x['project_name'], x['address'], x['long'], x['lat'])),
        axis=1
    ) 
      # drop column used to map project ID
      property_df = property_df.drop(['project_name', 'long', 'lat', 'address'], axis=1)
      return property_df

    def load_hdb_ura_to_transaction(self, hdb_filepath, ura_filepath):
      dbretrieve = RetrieveDB()
      hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      transaction_df = pd.concat([self._get_transaction_helper(ura), self._get_transaction_helper(hdb)]).reset_index(drop=True)
      # replace pd.NA with None for inserting into SQL
      transaction_df = transaction_df.replace({np.nan: None})
      transaction_df['property_id'] = dbretrieve.get_next_transaction_id() + transaction_df.index
      return transaction_df
    
    def load_amenities_df(self, amenities_filepath):
      dbretrieve = RetrieveDB()
      amenities_df = pd.read_csv(amenities_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
      amenities_df.drop_duplicates(subset = ['lat', 'long', 'amenity_type', 'amenity_name'], inplace=True)
      district_name_to_id_mapping = dbretrieve.get_district_name_to_id_mapping()
      amenities_df['district_id'] = amenities_df['district_name'].map(district_name_to_id_mapping)
      # drop column used to map district ID
      amenities_df = amenities_df.drop('district_name', axis=1)
      return amenities_df
