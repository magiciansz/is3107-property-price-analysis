from airflow.decorators import dag, task
from datetime import datetime, timedelta
#import json to process API responses
import json
#import requests to handle API calls
import requests
#import pandas for data wrangling
import pandas as pd

from etl_helper import one_map_authorise, assign_long_lat_to_private_property_dataset, assign_planning_area_to_private_property_dataset, assign_long_lat_to_hdb_dataset, assign_planning_area

################################### KEYS #######################################
# fill in following. Running API calls to get access tokens through VS Code / Collab always results in errors, use Postman

# GET https://www.ura.gov.sg/uraDataService/insertNewToken.action -H "AccessKey: accesskey"
ONEMAP_USERNAME = ""
ONEMAP_PASSWORD = ""

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='is3107_project_etl', default_args=default_args, schedule=None, catchup=False, tags=['final_project'])
def property_prices_etl():
    onemap_token = ''
    @task
    def authorise():
        onemap_token = one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
        return onemap_token
    @task
    def extract():
        private_property_dataset_paths = ['privatepropertypricesbatch1(1).json', 'privatepropertypricesbatch2(1).json', 'privatepropertypricesbatch3(1).json', 'privatepropertypricesbatch4(1).json']
        hdb_resale_dataset_path = 'ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv'
        return private_property_dataset_paths, hdb_resale_dataset_path
    def transform(private_property_dataset_paths, hdb_resale_dataset_path):
        private_property_dataset_edited_paths = ['privatepropertypricesbatch1edited.json', 'privatepropertypricesbatch2edited.json'
                                                 , 'privatepropertypricesbatch3edited.json', 'privatepropertypricesbatch4edited.json']
        private_property_datasets = []
        # open private property files, convert them into dictionaries from JSON
        for path in private_property_dataset_paths:
            with open(path, 'r') as f:
                private_property_datasets.append(json.load(f)['Result'])
        # add long, lat and planning area into dictionaries for private properties
        for count, dataset in enumerate(private_property_datasets):
            dataset = assign_long_lat_to_private_property_dataset(dataset, onemap_token)
            dataset = assign_planning_area_to_private_property_dataset(dataset, onemap_token)
            with open(private_property_dataset_edited_paths[count], 'w') as file:
                  file.write(json.dumps({'Status': 'Success', 'Result': dataset}))

        # open hdb resale dataset
        with open(hdb_resale_dataset_path, 'r') as f:
            hdb_dataset = pd.read_csv(hdb_resale_dataset_path)

            #filter for relevant date
            hdb_dataset = hdb_dataset[hdb_dataset['month'] >= '2019-02']
            hdb_dataset = hdb_dataset[hdb_dataset['month'] <= '2024-01']

            #add lat, long to HDB data
            hdb = assign_long_lat_to_hdb_dataset(hdb_dataset)

            #reassign index column
            hdb = hdb.set_index(hdb["Unnamed: 0"], drop=True)
            hdb.index.name = None
            hdb = hdb.drop("Unnamed: 0", axis=1)

            # add planning area into hdb resale CSV
            hdb = assign_planning_area_to_hdb_dataset(hdb, onemap_token)

            #output to csv         
            hdb.to_csv('hdb_with_planning_area.csv')         
        
        # massage private properties dataset
                  
        # massage hdb resale dataset
    def load():
        pass
    onemap_token =  authorise()   
    private_property_dataset_paths, hdb_resale_dataset_path = extract()
    transform(private_property_dataset_paths, hdb_resale_dataset_path, onemap_token)