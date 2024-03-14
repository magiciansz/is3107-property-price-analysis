import json
import requests

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


# this function takes in two values, x and y coordinates.
# it returns two strings: the latitude and longitude corresponding to these x and y coordinates
def coordinates_to_lat_long(x, y, ONEMAP_TOKEN):
    location = x + ',' + y
    url = 'https://www.onemap.gov.sg/api/public/revgeocodexy'
    headers = {"Authorization": ONEMAP_TOKEN}
    params = {
        'location': location,
        'buffer': 40,
        'addressType': 'All',
        'otherFeatures': 'N'
    }
    
    try:
      response = requests.get(url, params=params, headers=headers)
      response = response.json()['GeocodeInfo']

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
  for property in dataset:
    if 'x' in property and 'y' in property:
      property['lat'], property['long'] = coordinates_to_lat_long(property['x'], property['y'], ONEMAP_TOKEN)
    return dataset

#this function takes in a URA dataset, and assigns the planning area based on the helper function above
def assign_planning_area_to_private_property_dataset(dataset, ONEMAP_TOKEN):
  for property in dataset:
    if 'lat' in property and 'long' in property:
      property['planning_area'] = get_planning_area_name_from_lat_long(property['lat'], property['long'], ONEMAP_TOKEN)
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
    return ("NA", "NA")
  else:
    return lat, long

# this function takes in a HDB dataset, and assigns the lat and long of first search result based on concatenated address, block and street name columns
def assign_long_lat_to_hdb_dataset(dataset):
  for property in dataset:
    search_term = property["address"] = property["block"] + " " + property["street_name"]
    property['lat'], property['long'] = addr_to_lat_long(search_term)
    return dataset

#this function takes in a URA OR HDB dataset, and assigns the planning area based on the helper function above
def assign_planning_area(dataset, ONEMAP_TOKEN):
  return assign_planning_area_to_private_property_dataset(dataset, ONEMAP_TOKEN)