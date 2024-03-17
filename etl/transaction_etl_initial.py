from airflow.decorators import dag, task
from datetime import datetime, timedelta
#import json to process API responses
import json
#import requests to handle API calls
import requests
#import pandas for data wrangling
import pandas as pd

from etl_helper import one_map_authorise, ura_authorise, extract_private_property_data, assign_long_lat_to_private_property_dataset, assign_planning_area_to_private_property_dataset, assign_long_lat_to_hdb_dataset, assign_planning_area_to_hdb_dataset, load_hdb_ura_to_project, load_hdb_ura_to_property, load_hdb_ura_to_transaction
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
# save username and password to .env and run
ONEMAP_USERNAME = os.environ['ONEMAP_USERNAME']
ONEMAP_PASSWORD = os.environ['ONEMAP_PASSWORD']
URA_ACCESS_KEY = os.environ['URA_ACCESS_KEY']
URA_BATCHES = [1, 2, 3, 4]
URA_EXTRACT_PATH = 'privatepropertyprices'
URA_ADDED_FIELDS_PATH = 'privatepropertypricesadded'
DATA_FOLDER = "../Data"
HDB_DATA = 'ResaleflatpricesbasedonregistrationdatefromJan2017onwards.csv'
HDB_PATH = DATA_FOLDER / HDB_DATA
URA_FILETYPE = 'json'
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
    def extract(ura_access_token):
        # TODO maybe use API here & define filenames outside
        private_property_prices_data = {'Status': 'Success', 'Result': []}
        for batch in URA_BATCHES:
                data = extract_private_property_data(batch, URA_ACCESS_KEY, ura_access_token)
                for entry in data['Result']:
                        private_property_prices_data['Result'].append(entry)
                
        private_property_prices_dataset_path = DATA_FOLDER + '/' + URA_EXTRACT_PATH + '.json'
        with open(private_property_prices_dataset_path, 'w') as f:
                json.dump(private_property_prices_data, f)
        hdb_resale_dataset_path = HDB_PATH
        return private_property_prices_dataset_path, hdb_resale_dataset_path
    
    def transform(private_property_prices_dataset_path, hdb_resale_dataset_path, onemap_access_token):
        # open private property files, convert them into dictionaries from JSON
        private_property_prices_dataset_final_path = DATA_FOLDER + '/' + URA_ADDED_FIELDS_PATH + '.json'
        with open(private_property_prices_dataset_path, 'r') as f:
                dataset = json.load(f)['Result']
                dataset = assign_long_lat_to_private_property_dataset(dataset, onemap_access_token)
                dataset = assign_planning_area_to_private_property_dataset(dataset, onemap_access_token)
                
                with open(private_property_prices_dataset_final_path, 'w') as file:
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
            hdb = assign_planning_area_to_hdb_dataset(hdb, onemap_access_token)

            #output to csv         
            hdb.to_csv('hdb_with_planning_area.csv')
            
        # massage private properties dataset
            # TODO check w another team on private_property_dataset_edited_paths
            URA_combined_df = kml.URA_data_transformation_pipeline(DATA_FOLDER, URA_ADDED_FIELDS_PATH, URA_FILETYPE)
            URA_path_to_save = "{DATA_FOLDER}/URA_combined_df.csv"
            URA_combined_df.to_csv(URA_path_to_save, index = False)
            
        # massage hdb resale dataset
            hdb = kml.parse_hdb("hdb_with_planning_area.csv")    
            hdb_path_to_save = "{DATA_FOLDER}/hdb_clean.csv"
            hdb.to_csv(hdb_path_to_save, index=False)
        
        return URA_path_to_save, hdb_path_to_save
    
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