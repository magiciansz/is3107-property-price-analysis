import json
import requests
import pandas as pd
from RetrieveDB import RetrieveDB
getdb = RetrieveDB()
import time


#this function takes in your OneMap API username, password
#returns access token string

def one_map_authorise(username, password):
  auth_url = "https://www.onemap.gov.sg/api/auth/post/getToken"

  payload = {
          "email": username,
          "password": password
          }

  response = requests.request("POST", auth_url, json=payload)
  json_data = json.loads(response.text)

  return json_data['access_token']

def ura_authorise(ura_access_key):
  auth_url = 'https://www.ura.gov.sg/uraDataService/insertNewToken.action'
  headers = {'AccessKey': ura_access_key, 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
  response = requests.post(auth_url, headers=headers)
  return response.json()['Result']


def extract_private_property_data(batch_no, ura_access_key, ura_access_token):
    url = 'https://www.ura.gov.sg/uraDataService/invokeUraDS?service=PMI_Resi_Transaction'
    headers = {'AccessKey': ura_access_key, 'Token': ura_access_token, 'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
    params = {'batch': batch_no}
    data = requests.get(url, headers=headers, params=params)
    return data.json()

# this function takes in two values, x and y coordinates.
# it returns two strings: the latitude and longitude corresponding to these x and y coordinates
def coordinates_to_lat_long(x, y, ONEMAP_TOKEN):
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
##returns two strings: lat and long values of the property respectively
def get_planning_area_name_from_lat_long(lat, long, ONEMAP_TOKEN):
  if (lat=="NA" or long=="NA"):
    return "NA"

  planning_query_url = "https://www.onemap.gov.sg/api/public/popapi/getPlanningarea"
  headers = {"Authorization": ONEMAP_TOKEN}
  params = {
          "latitude": lat,
          "longitude": long
          }

  response = requests.request("GET", planning_query_url, params = params, headers=headers)
  try:
    json_data = json.loads(response.text)
  except:
    return "NA"
  else:
    try: 
      json_data[0]['pln_area_n']
    except KeyError:
      return "NA"
    else:
      return json_data[0]['pln_area_n']
    
# this function takes in a URA dataset, and assigns the lat and long based on the helper function above.
def assign_long_lat_to_private_property_dataset(dataset, ONEMAP_TOKEN):
  function_start = time.time()
  coordinates_mapping = {}
  total_entries = len(dataset)
  for count, property in enumerate(dataset):
    start = time.time()
    print('progress: ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion')
    if 'x' in property and 'y' in property:
      x, y = property['x'], property['y']
      if (x, y) not in coordinates_mapping:
          coordinates_mapping[(x, y)] = coordinates_to_lat_long(x, y, ONEMAP_TOKEN)
      property['lat'], property['long'] = coordinates_mapping[(x, y)]
    else:
      property['lat'], property['long'] = "NA", "NA"
    end = time.time()
    print(property['lat'], property['long'])
    print('time taken for ' + str(count) + '/' + str(total_entries) + ' for lat-long conversion is ' + str(end - start))
  function_end = time.time()
  print('time taken for whole function is ' + str(function_end - function_start))
  return dataset

def assign_planning_area_to_private_property_dataset(dataset, ONEMAP_TOKEN):
  function_start = time.time()
  coordinates_to_district_mapping = {}
  total_entries = len(dataset)
  for count, property in enumerate(dataset):
    start = time.time()
    print('progress: ' + str(count) + '/' + str(total_entries) + ' for planning area')
    lat, long = property['lat'], property['long']
    if (lat, long) not in coordinates_to_district_mapping:
      coordinates_to_district_mapping[(lat, long)] = get_planning_area_name_from_lat_long(property['lat'], property['long'], ONEMAP_TOKEN)
    property['planning_area'] = coordinates_to_district_mapping[(lat, long)]
    end = time.time()
    print(property['planning_area'])
    print('time taken for ' + str(count) + '/' + str(total_entries) + ' for planning area is ' + str(end - start))
  function_end = time.time()
  print('time taken for whole function is ' + str(function_end - function_start))
  return dataset

#this function takes in a search string (assumes previously concatenated by user),
##returns two strings: lat and long values of the property respectively
def addr_to_lat_long(search_term):
  data = {
    "searchVal": search_term,
    "returnGeom": "Y",
    "getAddrDetails": "Y"
    }
  search_url = "https://www.onemap.gov.sg/api/common/elastic/search"

  response = requests.request("GET", search_url, params=data)
  #attempt to process API response and assign lat and long values
  try:
    json_data = json.loads(response.text)
    lat = json_data["results"][0]["LATITUDE"]
    long = json_data["results"][0]["LONGITUDE"]
  #if json processing fails (for reasons such as empty json response due to invalid address) return "NA" for lat and long
  except:
    return "NA", "NA"
  else:
    return lat, long

# this function takes in a HDB dataset, and assigns the lat and long of first search result based on concatenated address, block and street name columns
def assign_long_lat_to_hdb_dataset(dataset):
  dataset['full_address'] = dataset["block"] + " " + dataset["street_name"]
  dataset[['lat', 'long']] = dataset[['full_address']].apply(addr_to_lat_long, axis=1, result_type='expand')
  dataset= dataset.drop('full_address', axis=1)
  return dataset

#this function takes in a HDB dataset, and assigns the planning area based on the helper function above
def assign_planning_area_to_hdb_dataset(dataset, ONEMAP_TOKEN):
  dataset['planning_area'] = dataset.apply(lambda x: get_planning_area_name_from_lat_long(x.lat, x.long, ONEMAP_TOKEN), axis=1)
  return dataset

  
def _get_projects_helper(df, district_mapping):
  # TODO district_mapping is expected to be a df extracted from onemap api containing cols district_name and district_id
  # NOT NULL columns
  df.dropna(subset=['district_name', 'project_name'], inplace=True)
  project_cols = ['project_name', 'district_name', 'long', 'lat']
  df.drop_duplicates(subset = ['project_name'], inplace=True)
  df = df.reset_index()[project_cols]
  df = pd.merge(df, district_mapping, how='left', on=['district_name']).drop(columns=['district_name'])  
  return df

def _get_property_helper(df):
  property_cols = ['project_id', 'property_type', 'street', 'lease_year', 'lease_duration', 'floor_range_start', 'floor_range_end', 'floor_area']
  df['project_id'] = df['project_name'].apply(getdb.get_project_id)
  df = df[property_cols]
  return df

def _get_transaction_helper(df):
  tx_cols = ['property_id', 'transaction_year', 'transaction_month', 'type_of_sale', 'price']
  id_values = []
  for row in df.itertuples():
    val = getdb.get_property_id(row.project_id, row.property_type, row.street, row.lease_year, row.lease_duration, row.floor_range_start, row.floor_range_end, row.floor_area)
    id_values.append(val)
  df['property_id'] = id_values
  df = df[tx_cols]
  return df

def load_hdb_ura_to_project(hdb_filepath, ura_filepath):
  # cleaned datasets
  hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
  ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')

  # prepare data for Project table
  project_df = pd.concat([_get_projects_helper(ura), _get_projects_helper(hdb)])

  return project_df

def load_hdb_ura_to_property(hdb_filepath, ura_filepath):
  hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
  ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
  property_df = pd.concat([_get_property_helper(ura), _get_property_helper(hdb)])
  return property_df

def load_hdb_ura_to_transaction(hdb_filepath, ura_filepath):
  hdb = pd.read_csv(hdb_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
  ura = pd.read_csv(ura_filepath).drop("Unnamed: 0", axis=1, errors='ignore')
  transaction_df = pd.concat([_get_transaction_helper(ura), _get_transaction_helper(hdb)])
  return transaction_df
