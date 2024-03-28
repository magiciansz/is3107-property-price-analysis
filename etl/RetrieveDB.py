import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql
from datetime import datetime
from google.cloud.sql.connector import Connector, IPTypes
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

# local connection credentials
MYSQL_USERNAME = os.environ['MYSQL_USERNAME']
MYSQL_PASSWORD=os.environ['MYSQL_PASSWORD']
MYSQL_HOST= os.environ['MYSQL_HOST']
MYSQL_PORT = os.environ['MYSQL_PORT']
MYSQL_DATABASE_NAME=os.environ['MYSQL_DATABASE_NAME']

# cloud connection credentials
GOOGLE_APPLICATION_CREDENTIALS_NAME = os.environ['GOOGLE_APPLICATION_CREDENTIALS_NAME']
GOOGLE_APPLICATION_CREDENTIALS = Path(__file__).parent.parent / GOOGLE_APPLICATION_CREDENTIALS_NAME
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GOOGLE_APPLICATION_CREDENTIALS)
INSTANCE_CONNECTION_NAME = os.environ['INSTANCE_CONNECTION_NAME']
DB_IAM_USER = os.environ['DB_IAM_USER']
DB_NAME = os.environ['DB_NAME']


class RetrieveDB:
    def __init__(self, db_connect_type):
        self.date = datetime.now().strftime("%Y-%m-%d")
        self.db_connect_type = db_connect_type
        self.conn = self.__connect_to_db(db_connect_type)
            
    def __connect_to_db(self, type) -> pymysql.connections.Connection:
        """adjust the type of connections to diff databases

        Args:
            type (connection type): local or gcp

        Returns:
            pymysql.connections.Connection: the connection object
        """
        # Connect to local mysql database
        if type == "LOCAL":
            engine = sqlalchemy.create_engine(
                url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                    MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE_NAME
                )
            )
            try:
                # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
                conn = engine.connect()
                print(
                    f"Connection to the {MYSQL_HOST} for user {MYSQL_USERNAME} created successfully with {conn}.")
                return conn
            except Exception as ex:
                print("Connection could not be made due to the following error: \n", ex)
                
        # Connect to cloud mysql database using IAM credentials
        if type == "IAM":
            def get_conn() -> pymysql.connections.Connection:
                connector = Connector()
                conn: pymysql.connections.Connection = connector.connect(
                    INSTANCE_CONNECTION_NAME,
                    "pymysql",
                    user=DB_IAM_USER,
                    db=DB_NAME,
                    enable_iam_auth=True
                )
                return conn            
            engine = sqlalchemy.create_engine(
                "mysql+pymysql://",
                creator=get_conn,
                future=True
            )
            try:
                # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
                conn = engine.connect()
                print(
                    f"Connection to the {INSTANCE_CONNECTION_NAME} for user {DB_IAM_USER} created successfully with {conn}.")
                return conn
            except Exception as ex:
                print("Connection could not be made due to the following error: \n", ex)

    def get_district_name_to_id_mapping(self):
        query = sqlalchemy.text("""
                                SELECT id, district_name
                                FROM District
                               """)
        results = self.conn.execute(query)
        district_name_to_id_mapping = {}
        for id, district_name in results:
            district_name_to_id_mapping[district_name] = id
        return district_name_to_id_mapping
    
    def get_project_details_to_id_mapping(self):
        query = sqlalchemy.text("""
                                SELECT id, project_name, `address`, `long`, lat
                                FROM Project
                               """)
        results = self.conn.execute(query)
        project_details_to_id_mapping = {}
        for id, project_name, address, long, lat in results:
            project_details_to_id_mapping[(project_name, address, long, lat)] = id
        return project_details_to_id_mapping
    
    def get_next_transaction_id(self):
        query = sqlalchemy.text("""
                                SELECT max(id)
                                FROM Transaction
                               """)
        results = self.conn.execute(query)
        id = results.fetchone()[0]
        if not id:
            return 1
        return id + 1
    
if __name__ == "__main__":
    db = RetrieveDB(db_connect_type="IAM")