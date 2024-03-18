from airflow.decorators import dag, task
from datetime import datetime, timedelta
#import json to process API responses
import json
#import requests to handle API calls
import requests
#import pandas for data wrangling
import pandas as pd
#import time to track when is point of update (for hdb dataset) in order to pull year-month of interest
import time

from etl_helper import one_map_authorise, filter_current_month_dataset, ura_authorise, get_list_of_year_months, extract_hdb_data, extract_private_property_data, assign_long_lat_to_private_property_dataset, assign_planning_area_to_private_property_dataset, assign_long_lat_to_hdb_dataset, assign_planning_area_to_hdb_dataset, load_hdb_ura_to_project, load_hdb_ura_to_property, load_hdb_ura_to_transaction
import sys
from ..src.DataParser import DataParser
from ..src.UpdateDB import UpdateDB
CREATE_TABLES_SQL_PATH = 'src/create_tables_clean.sql'

dbupdate = UpdateDB()
kml = DataParser()

import os
from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

################################### KEYS #######################################
# fill in following. Running API calls to get access tokens through VS Code / Collab always results in errors, use Postman

# GET https://www.ura.gov.sg/uraDataService/insertNewToken.action -H "AccessKey: accesskey"
##access keys
ONEMAP_USERNAME = os.environ['ONEMAP_USERNAME']
ONEMAP_PASSWORD = os.environ['ONEMAP_PASSWORD']
URA_ACCESS_KEY = os.environ['URA_ACCESS_KEY']
#common vars
DATA_FOLDER = "../Data"
#ura vars
URA_BATCHES = [1, 2, 3, 4]
URA_EXTRACT_PATH = 'privatepropertypricesupdate'
URA_ADDED_FIELDS_PATH = 'privatepropertypricesupdateadded'
URA_FILETYPE = 'json'

#hdb vars
QUERY_YEAR_MONTH_HDB = '2019-02'
HDB_EXTRACT_PATH = 'hdb_prices_update' '_' + QUERY_YEAR_MONTH_HDB
HDB_ADDED_FIELDS_PATH = 'hdb_prices_update_added'+ '_' + QUERY_YEAR_MONTH_HDB



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
    @task
    def authorise():
        onemap_access_token = one_map_authorise(ONEMAP_USERNAME, ONEMAP_PASSWORD)
        ura_access_token = ura_authorise(URA_ACCESS_KEY)
        return onemap_access_token, ura_access_token
    
    @task
    def create_tables_db(create_tables_sql):
        """create tables in db

        Args:
            create_tables_sql (create tables sql path): CREATE_TABLES_SQL_PATH
        """
        dbupdate.create_tables(create_tables_sql)

        # next step: create tables GCP
        pass


    @task
    def extract_ura(ura_access_token):
        # TODO maybe use API here & define filenames outside
        private_property_prices_data = {'Status': 'Success', 'Result': []}
        for batch in URA_BATCHES:
                data = extract_private_property_data(batch, URA_ACCESS_KEY, ura_access_token)
                for entry in data['Result']:
                        private_property_prices_data['Result'].append(entry)
                
        private_property_prices_dataset_path = DATA_FOLDER + '/' + URA_EXTRACT_PATH + '.json'
        with open(private_property_prices_dataset_path, 'w') as f:
                json.dump(private_property_prices_data, f)
        
        return private_property_prices_dataset_path
    
    @task
    def extract_hdb():
        yesterday_calc = datetime.today() - timedelta(days=1)
        yesterday = yesterday_calc.strftime("%Y-%m")
        list_of_year_months_to_date = get_list_of_year_months(yesterday, yesterday)
        #initilize dict to store results
        hdb_api = []
        #get hdb data for all months using API (initialization)
        for m in list_of_year_months_to_date:
            hdb_api.extend(extract_hdb_data(m))
        hdb_prices_data = {}
        hdb_prices_data['Result'] = hdb_api
        hdb_prices_dataset_path = DATA_FOLDER + '/' + HDB_EXTRACT_PATH + '.json'
        with open(hdb_prices_dataset_path, 'w') as f:
                json.dump(hdb_prices_data, f)
        return hdb_prices_dataset_path
         
    @task
    def transform_ura(private_property_prices_dataset_path, onemap_access_token):
        # open private property files, convert them into dictionaries from JSON
        private_property_prices_dataset_final_path = DATA_FOLDER + '/' + URA_ADDED_FIELDS_PATH + '.json'
        with open(private_property_prices_dataset_path, 'r') as f:
                dataset = json.load(f)['Result']
                dataset = filter_current_month_dataset(dataset)
                print(dataset)
                dataset = assign_long_lat_to_private_property_dataset(dataset, onemap_access_token)
                dataset = assign_planning_area_to_private_property_dataset(dataset, onemap_access_token)
                with open(private_property_prices_dataset_final_path, 'w') as file:
                        file.write(json.dumps({'Status': 'Success', 'Result': dataset}))

        # massage private properties dataset
        # TODO check w another team on private_property_dataset_edited_paths
        URA_combined_df = kml.URA_data_transformation_pipeline(DATA_FOLDER, URA_ADDED_FIELDS_PATH, URA_FILETYPE)
        URA_path_to_save = "{DATA_FOLDER}/URA_combined_df.csv"
        URA_combined_df.to_csv(URA_path_to_save, index = False)
            
        
        return URA_path_to_save
    
    @task
    def transform_hdb(hdb_prices_dataset_path, onemap_access_token):
        hdb_prices_dataset_final_path = DATA_FOLDER + '/' + HDB_ADDED_FIELDS_PATH + '.json'
        with open(hdb_prices_dataset_path, 'r') as f:
            dataset = json.load(f)
            dataset = assign_long_lat_to_hdb_dataset(dataset)
            dataset = assign_planning_area_to_hdb_dataset(dataset, onemap_access_token)
            
            with open(hdb_prices_dataset_final_path, 'w') as file:
                file.write(json.dumps(dataset))
        # massage hdb resale dataset
        hdb = kml.parse_hdb("hdb_with_planning_area.csv")    
        hdb_path_to_save = "{DATA_FOLDER}/hdb_clean.csv"
        hdb.to_csv(hdb_path_to_save, index=False)
        return hdb_path_to_save
        
   
    @task
    def load_district(district_path):
        # should be the first table to populate data
        district = pd.read_csv(district_path, index = False)
        dbupdate.update_district_table(district)
        return district_path
    
    @task
    def load_amenities(district_path):
        # should be after district table, can be run concurrently with load_transactions
        pass


    @task
    def load_transactions(district_path, URA_path_to_save, hdb_path_to_save):
        # TODO
        # for tables project, property and transaction, can be run concurrently with load_amenities

        project_df = load_hdb_ura_to_project(hdb_path_to_save, URA_path_to_save)
        dbupdate.update_project_table(project_df)

        property_df = load_hdb_ura_to_property(hdb_path_to_save, URA_path_to_save)
        # 
        
        transaction_df = load_hdb_ura_to_transaction(hdb_path_to_save, URA_path_to_save)
        dbupdate.update_transaction_table(transaction_df)

        pass


    onemap_access_token, ura_access_token =  authorise()
    # private_property_dataset_paths, hdb_resale_dataset_path = extract()
    # transform(private_property_dataset_paths, hdb_resale_dataset_path, onemap_token)

# end define DAG

property_prices_etl = property_prices_etl()
