from airflow.decorators import dag, task
from datetime import datetime, timedelta
#import json to process API responses
import json
from json import JSONDecodeError
#import requests to handle API calls
import requests
#import kaggle to handle kaggle API calls
import kaggle
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

dbupdate = UpdateDB("LOCAL")
dbretrieve = RetrieveDB("LOCAL")
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
# end define variables


# DAG def start
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

@dag(dag_id='is3107_project_initial_etl', default_args=default_args, schedule=None, catchup=False, tags=['final_project'])
def property_prices_initial_etl():

    @task
    def authorise_onemap():
         onemap_access_token = etl_helper.one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
         return onemap_access_token

    @task
    def authorise_ura():
         ura_access_token = etl_helper.ura_authorise(URA_ACCESS_KEY)
         return ura_access_token
    
    @task
    def create_tables_db(upstream_task_flag, create_tables_sql):
        """create tables in db

        Args:
            create_tables_sql (create tables sql path): CREATE_TABLES_SQL_PATH
        """
        dbupdate.create_tables(create_tables_sql)
        return

    @task
    def extract_planning_area(onemap_access_token):
        return etl_helper.extract_planning_area_polygon(onemap_access_token, "2024")
    
           
    @task
    def extract_ura(upstream_task_flag, ura_access_token):
        # TODO maybe use API here & define filenames outside
        ura_prices_data = {'Result': []}
        for batch in URA_BATCHES:
                data = etl_helper.extract_ura_data(batch, URA_ACCESS_KEY, ura_access_token)
                for entry in data['Result']:
                        ura_prices_data['Result'].append(entry)
                
        ura_prices_dataset_path = "{DATA_FOLDER}/ura_prices.json".format(DATA_FOLDER = DATA_FOLDER)
        with open(ura_prices_dataset_path, 'w') as f:
                json.dump(ura_prices_data, f)
        
        return ura_prices_dataset_path
    
    @task
    def extract_hdb(upstream_task_flag):
        today = time.strftime("%Y-%m")
        list_of_year_months_to_date = etl_helper.get_list_of_year_months(START_YEAR_MONTH_HDB, today)
        #initilize dict to store results
        hdb_api = []
        #get hdb data for all months using API (initialization)
        for m in list_of_year_months_to_date:
            hdb_api.extend(etl_helper.extract_hdb_data(m))
        hdb_prices_dataset_path = "{DATA_FOLDER}/hdb_prices.json".format(DATA_FOLDER = DATA_FOLDER)
        hdb_prices_data = {}
        hdb_prices_data['Result'] = hdb_api
        with open(hdb_prices_dataset_path, 'w') as f:
                json.dump(hdb_prices_data, f)
        return hdb_prices_dataset_path

    @task
    def extract_amenities(upstream_task_flag):
        amenities_dataset_path_dict = kml.download_amenity_files(output_folder = DATA_FOLDER, first_time = True)
        return amenities_dataset_path_dict
        
    @task
    def extract_districts(upstream_task_flag, onemap_access_token):
        districts_dataset_path = "{DATA_FOLDER}/districts_initial.json".format(DATA_FOLDER = DATA_FOLDER)
        with open(districts_dataset_path, 'w') as f:
            json.dump({'results': etl_helper.extract_planning_area_polygon(onemap_access_token)}, f)
        return districts_dataset_path
         
    @task
    def transform_ura(upstream_task_flag, ura_prices_dataset_path, onemap_access_token):
        # open private property files, convert them into dictionaries from JSON
        ura_prices_dataset_final_path = "{DATA_FOLDER}/ura_prices_added.json".format(DATA_FOLDER = DATA_FOLDER)
        with open(ura_prices_dataset_path, 'r') as f:
            dataset = json.load(f)['Result']
            dataset = etl_helper.assign_long_lat_to_ura_dataset(dataset, onemap_access_token)
            dataset = etl_helper.assign_planning_area_to_ura_dataset(dataset, onemap_access_token)
            
            with open(ura_prices_dataset_final_path, 'w') as file:
                    file.write(json.dumps({'Result': dataset}))

        # massage hdb dataset
        ura_combined_df = kml.URA_data_transformation_pipeline(ura_prices_dataset_final_path)
        ura_combined_df_path = "{DATA_FOLDER}/URA_combined_df.csv".format(DATA_FOLDER = DATA_FOLDER)
        ura_combined_df.to_csv(ura_combined_df_path, index = False)
            
        return ura_combined_df_path
    
    @task
    def transform_hdb(upstream_task_flag, hdb_prices_dataset_path, onemap_access_token):
        hdb_prices_dataset_final_path = "{DATA_FOLDER}/hdb_prices_added.json".format(DATA_FOLDER = DATA_FOLDER)
        with open(hdb_prices_dataset_path, 'r') as f:
            dataset = json.load(f)['Result']
            dataset = etl_helper.assign_long_lat_to_hdb_dataset(dataset)
            dataset = etl_helper.assign_planning_area_to_hdb_dataset(dataset, onemap_access_token)
            
            with open(hdb_prices_dataset_final_path, 'w') as file:
                file.write(json.dumps({'Result': dataset}))
        # massage hdb resale dataset
        hdb_combined_df = kml.parse_hdb(hdb_prices_dataset_final_path)  
        hdb_combined_df_path = "{DATA_FOLDER}/hdb_combined_df.csv".format(DATA_FOLDER = DATA_FOLDER)
        hdb_combined_df.to_csv(hdb_combined_df_path, index=False)
        return hdb_combined_df_path

    @task
    def transform_amenities(upstream_task_flag, amenities_dataset_path_dict, onemap_access_token):
        amenity_src_folder_path = DATA_FOLDER
        amenity_out_folder_path = amenity_src_folder_path

        combined_amenities_file_path = kml.amenity_data_transformation_pipeline(amenities_dataset_path_dict, amenity_src_folder_path, amenity_out_folder_path, onemap_access_token)
        return combined_amenities_file_path
    
    @task
    def transform_districts(upstream_task_flag, districts_dataset_path):
        x = pd.read_json(districts_dataset_path)
        x['pln_area_n'] = x['results'].apply(lambda x: x['pln_area_n'])
        x['coord_list'] = x['results'].apply(lambda x: eval(x['geojson'])['coordinates'][0][0])
        x = x.drop("results", axis=1)
        x_dict = x.to_dict()
        districts_dataset_final_path = "{DATA_FOLDER}/districts_initial_added.json".format(DATA_FOLDER = DATA_FOLDER)
        with open(districts_dataset_final_path, 'w') as file:
            file.write(json.dumps({'Result': x_dict}))
        return districts_dataset_final_path
         
    @task
    def load_districts(upstream_task_flag, district_path):
        district_df = etl_helper.load_district_df(district_path)
        dbupdate.load_district_table(district_df)
        return
    @task
    def load_projects(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        project_df = etl_helper.load_hdb_ura_to_project(hdb_combined_df_path, ura_combined_df_path)
        dbupdate.load_project_table(project_df)
        return
    @task
    def load_properties(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        property_df = etl_helper.load_hdb_ura_to_property(hdb_combined_df_path, ura_combined_df_path)
        dbupdate.load_property_table(property_df)
        return
    @task
    def load_transactions(upstream_task_flag, hdb_combined_df_path, ura_combined_df_path):
        transaction_df = etl_helper.load_hdb_ura_to_transaction(hdb_combined_df_path, ura_combined_df_path)
        dbupdate.load_transaction_table(transaction_df)
        return transaction_df
    
    @task
    def load_amenities(upstream_task_flag, amenities_combined_df_path):
        amenities_df = etl_helper.load_amenities_df(amenities_combined_df_path)
        dbupdate.load_amenity_table(amenities_df)
        return

    # from scratch
    onemap_access_token = authorise_onemap()
    ura_access_token =  authorise_ura()
    hdb_prices_dataset_path = extract_hdb(onemap_access_token)
    ura_prices_dataset_path = extract_ura(hdb_prices_dataset_path, ura_access_token)
    amenities_dataset_path_dict = extract_amenities(ura_prices_dataset_path)
    districts_dataset_path = extract_districts(amenities_dataset_path_dict, onemap_access_token)
    
    hdb_combined_df_path = transform_hdb(districts_dataset_path, hdb_prices_dataset_path, onemap_access_token)
    ura_combined_df_path = transform_ura(hdb_combined_df_path, ura_prices_dataset_path, onemap_access_token)
    amenities_combined_df_path = transform_amenities(ura_combined_df_path, amenities_dataset_path_dict, onemap_access_token)
    districts_final_dataset_path = transform_districts(amenities_combined_df_path, districts_dataset_path)
    
    # create tables + load
    tables_created_flag = create_tables_db(districts_final_dataset_path, CREATE_TABLES_SQL_PATH)

    districts_loaded_flag = load_districts(tables_created_flag, districts_final_dataset_path)
    projects_loaded_flag = load_projects(districts_loaded_flag, hdb_combined_df_path, ura_combined_df_path)
    properties_loaded_flag = load_properties(projects_loaded_flag, hdb_combined_df_path, ura_combined_df_path)
    transactions_loaded_flag = load_transactions(properties_loaded_flag, hdb_combined_df_path, ura_combined_df_path)
    load_amenities(transactions_loaded_flag, amenities_combined_df_path)

# end define DAG

property_prices_initial_etl = property_prices_initial_etl()