from airflow.decorators import dag, task
from datetime import datetime, timedelta
#import json to process API responses
import json
#import requests to handle API calls
#import kaggle to handle kaggle API calls
#import pandas for data wrangling
import pandas as pd
#import time to track when is point of initilization (for hdb dataset) in order to pull 2019-02 to current year-month
import time
#import tqdm to check progress of function execution
from tqdm import tqdm
tqdm.pandas()

from EtlHelper import EtlHelper
import sys

from DataParser import DataParser
from UpdateDB import UpdateDB
from RetrieveDB import RetrieveDB

dbupdate = UpdateDB("IAM")
dbretrieve = RetrieveDB("IAM")
etl_helper = EtlHelper()

kml = DataParser()
import os
from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())




################################### KEYS #######################################
# save username and password to .env and run
##access keys
ONEMAP_USERNAME = os.environ['ONEMAP_USERNAME']
ONEMAP_PASSWORD = os.environ['ONEMAP_PASSWORD']
URA_ACCESS_KEY = os.environ['URA_ACCESS_KEY']
#common vars
DATA_FOLDER = os.environ['DATA_FOLDER']
CREATE_TABLES_SQL_PATH = os.environ['CREATE_TABLES_SQL_PATH']
#districts vars
DISTRICTS_EXTRACT_PATH = 'districts_initial'
DISTRICTS_ADDED_FIELDS_PATH = 'districts_initial_added'
#URA vars
URA_BATCHES = [1, 2, 3, 4]
#hdb vars
START_YEAR_MONTH_HDB = '2019-02'
CURRENT_YEAR_MONTH = time.strftime("%Y-%m")

# end define variablese.strftime("%Y-%m")
# end define variables



default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}


@dag(dag_id='is3107_project_update_etl', default_args=default_args, schedule=None, catchup=False, tags=['final_project'])
def property_prices_update_etl():
    # @task
    # def authorise():
    #     onemap_access_token = etl_helper.one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
    #     ura_access_token = etl_helper.ura_authorise(URA_ACCESS_KEY)
    #     return onemap_access_token, ura_access_token

    @task
    def authorise_onemap():
         onemap_access_token = etl_helper.one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
         return onemap_access_token

    @task
    def authorise_ura():
         ura_access_token = etl_helper.ura_authorise(URA_ACCESS_KEY)
         return ura_access_token
        
    @task
    def extract_ura(upstream_task_flag, ura_access_token):
        current_month_year = time.strftime("%m%y")
        ura_prices_data = {'Result': []}
        for batch in URA_BATCHES:
                data = etl_helper.extract_ura_data(batch, URA_ACCESS_KEY, ura_access_token, current_month_year)
                for entry in data['Result']:
                        ura_prices_data['Result'].append(entry)
        ura_prices_dataset_path = "{DATA_FOLDER}/ura_prices_extract_{YEAR_MONTH}.json".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        with open(ura_prices_dataset_path, 'w') as f:
                json.dump(ura_prices_data, f)
        
        return ura_prices_dataset_path

    @task
    def extract_hdb(upstream_task_flag):
        #initilize dict to store results
        #get hdb data for current month using API (initialization)
        hdb_api = etl_helper.extract_hdb_data(CURRENT_YEAR_MONTH)
        hdb_prices_dataset_path = "{DATA_FOLDER}/hdb_prices_extract_{YEAR_MONTH}.json".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        hdb_prices_data = {}
        hdb_prices_data['Result'] = hdb_api
        with open(hdb_prices_dataset_path, 'w') as f:
                json.dump(hdb_prices_data, f)
        return hdb_prices_dataset_path

    @task
    def extract_amenities(upstream_task_flag):
        amenity_url_dict = kml.download_amenity_files(output_folder = DATA_FOLDER, first_time = False)
        return amenity_url_dict

    @task
    def transform_ura(upstream_task_flag, ura_prices_dataset_path, onemap_access_token):
        # open private property files, convert them into dictionaries from JSON
        ura_prices_dataset_final_path = "{DATA_FOLDER}/ura_prices_update_{YEAR_MONTH}.json".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        with open(ura_prices_dataset_path, 'r') as f:
            dataset = json.load(f)['Result']
            if not dataset:
                 print('No new URA properties detected.')
            dataset = etl_helper.assign_long_lat_to_ura_dataset(dataset, onemap_access_token)
            dataset = etl_helper.assign_planning_area_to_ura_dataset(dataset, onemap_access_token)
            with open(ura_prices_dataset_final_path, 'w') as file:
                    file.write(json.dumps({'Result': dataset}))

        # massage hdb dataset
        ura_combined_df = kml.URA_data_transformation_pipeline(ura_prices_dataset_final_path)
        ura_combined_df_path = "{DATA_FOLDER}/URA_combined_df_update_{YEAR_MONTH}.csv".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        ura_combined_df.to_csv(ura_combined_df_path, index = False)
            
        return ura_combined_df_path

    @task
    def transform_hdb(upstream_task_flag, hdb_prices_dataset_path, onemap_access_token):
        hdb_prices_dataset_final_path = "{DATA_FOLDER}/hdb_prices_update_{YEAR_MONTH}.json".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        with open(hdb_prices_dataset_path, 'r') as f:
            dataset = json.load(f)['Result']
            if not dataset:
                 print('No new HDB properties detected.')
            dataset = etl_helper.assign_long_lat_to_hdb_dataset(dataset)
            dataset = etl_helper.assign_planning_area_to_hdb_dataset(dataset, onemap_access_token)
            with open(hdb_prices_dataset_final_path, 'w') as file:
                file.write(json.dumps({'Result': dataset}))
        # massage hdb resale dataset
        hdb_combined_df = kml.parse_hdb(hdb_prices_dataset_final_path)  
        hdb_combined_df_path = "{DATA_FOLDER}/hdb_combined_df_update_{YEAR_MONTH}.csv".format(DATA_FOLDER = DATA_FOLDER, YEAR_MONTH = CURRENT_YEAR_MONTH)
        hdb_combined_df.to_csv(hdb_combined_df_path, index=False)
        return hdb_combined_df_path

    @task
    def transform_amenities(upstream_task_flag, amenity_url_dict, onemap_access_token):
        new_combined_df_path = kml.transform_amenity_files_pipeline(DATA_FOLDER, amenity_url_dict, onemap_access_token)
        try:
            new_combined_df = pd.read_csv(new_combined_df_path)
            if not new_combined_df.empty:
                print('New Amenities detected')
            else:
                print("No new amenities detected.")
        except pd.errors.EmptyDataError:
            print("No new amenities detected.")
        return new_combined_df_path
    
    @task
    def load_projects(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        project_df = etl_helper.load_hdb_ura_to_project(hdb_combined_df_path, ura_combined_df_path)
        if not project_df.empty:
            dbupdate.load_project_table(project_df)
        return

    @task
    def load_properties(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        property_df = etl_helper.load_hdb_ura_to_property(hdb_combined_df_path, ura_combined_df_path)
        if not property_df.empty:
            dbupdate.load_property_table(property_df)
        return

    @task
    def load_transactions(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        transaction_df = etl_helper.load_hdb_ura_to_transaction(hdb_combined_df_path, ura_combined_df_path)
        if not transaction_df.empty:
            dbupdate.load_transaction_table(transaction_df)
        return transaction_df

    @task
    def load_amenities(upstream_task_flag, amenities_combined_df_path):
        amenities_df = etl_helper.load_amenities_df(amenities_combined_df_path)
        if not amenities_df.empty:
            dbupdate.load_amenity_table(amenities_df)
        return
    
    # Execution pipeline
    onemap_access_token = authorise_onemap() 
    ura_access_token = authorise_ura()
    
    hdb_prices_dataset_path = extract_hdb(onemap_access_token)
    ura_prices_dataset_path = extract_ura(hdb_prices_dataset_path, ura_access_token)
    amenities_dataset_path_dict = extract_amenities(ura_prices_dataset_path)
    hdb_combined_df_path = transform_hdb(amenities_dataset_path_dict, hdb_prices_dataset_path, onemap_access_token)
    ura_combined_df_path = transform_ura(hdb_combined_df_path, ura_prices_dataset_path, onemap_access_token)
    amenities_combined_df_path = transform_amenities(ura_combined_df_path, amenities_dataset_path_dict, onemap_access_token)

    projects_loaded_flag = load_projects(amenities_combined_df_path, hdb_combined_df_path, ura_combined_df_path)
    properties_loaded_flag = load_properties(projects_loaded_flag, hdb_combined_df_path, ura_combined_df_path)
    transactions_loaded_flag = load_transactions(properties_loaded_flag, hdb_combined_df_path, ura_combined_df_path)
    load_amenities(transactions_loaded_flag, amenities_combined_df_path)

# end define DAG

property_prices_update_etl = property_prices_update_etl()