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
    
    def get_amenity_details_to_id_mapping(self):
        query = sqlalchemy.text("""
                                SELECT id, amenity_type, amenity_name, `long`, lat
                                FROM Amenity
                               """)
        results = self.conn.execute(query)
        amenity_details_to_id_mapping = {}
        for id, amenity_type, amenity_name, long, lat in results:
            amenity_details_to_id_mapping[(amenity_type, amenity_name, long, lat)] = id
        return amenity_details_to_id_mapping
    
    def get_merged_transactions(self):
        query = sqlalchemy.text(f"""
            SELECT proj.district_id, proj.long, proj.lat, 
                    tx.transaction_year, tx.transaction_month, tx.type_of_sale, tx.price,
                    prop.property_type, prop.lease_year, prop.lease_duration, prop.floor_range_start, prop.floor_range_end, prop.floor_area
            FROM Project proj
            LEFT JOIN Property prop ON proj.id = prop.project_id
            LEFT JOIN Transaction tx ON prop.id = tx.property_id
        """)
        
        df = pd.read_sql(query, self.conn)
        return df.to_dict('records')
    
    def get_amenities(self):
        query = sqlalchemy.text(f"""
            SELECT *
            FROM Amenity
        """)
        df = pd.read_sql(query, self.conn)
        return df.to_dict('records')

    def get_districts(self):
        query = sqlalchemy.text(f"""
            SELECT *
            FROM District
        """)
        df = pd.read_sql(query, self.conn)
        return df.to_dict('records')

    def get_amenity_of_type_for_ml(self, amenity_type):
        query = sqlalchemy.text(f"""
            SELECT district_id, `long`, `lat`
            FROM Amenity
            WHERE amenity_type = '{amenity_type}';
        """)
        df = pd.read_sql(query, self.conn)
        return df.to_dict('records')
    
    def get_price_per_sqft_dashboard(self):
        tx = self.get_merged_transactions()
        query = sqlalchemy.text(f"""
            SELECT proj.district_id, proj.long, proj.lat, 
                    tx.transaction_year, tx.transaction_month, tx.type_of_sale, tx.price,
                    prop.property_type, prop.lease_year, prop.lease_duration, prop.floor_range_start, prop.floor_range_end, prop.floor_area,
                    (tx.price / prop.floor_area) AS price_per_sqft
            FROM Project proj
            LEFT JOIN Property prop ON proj.id = prop.project_id
            LEFT JOIN Transaction tx ON prop.id = tx.property_id
            ORDER BY price_per_sqft
        """)
        df = pd.read_sql(query, self.conn)
        return df.to_dict('records')
    
    
if __name__ == "__main__":
    db = RetrieveDB(db_connect_type="IAM")
    # db = RetrieveDB(db_connect_type="LOCAL")
    # test = db.get_records_for_ml()
    # # test= db.get_amenity_of_type_for_ml("Kindergarten")
    # print(test)