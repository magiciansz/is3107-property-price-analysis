import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert
from datetime import datetime
from google.cloud.sql.connector import Connector, IPTypes
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())
CREATE_TABLES_SQL_PATH = 'src/create_tables_clean.sql'

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


class UpdateDB:
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
            self.engine = engine
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
            )
            self.engine = engine
            try:
                # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
                conn = engine.connect()
                print(
                    f"Connection to the {INSTANCE_CONNECTION_NAME} for user {DB_IAM_USER} created successfully with {conn}.")
                return conn
            except Exception as ex:
                print("Connection could not be made due to the following error: \n", ex)

    def create_tables(self, create_tables_sql_path):
        """Create tables in db

        Args:
            create_tables_sql (sql file): table creation scripts
        """
        with open(create_tables_sql_path, 'r') as create_tables_sql:
            queries = create_tables_sql.read().split(";")
            # print(queries)
            for query in queries:
                if query.strip():
                    query = sqlalchemy.text(query)
                    self.conn.execute(query)

    @compiles(Insert)
    def append_string(insert, compiler, **kw):
        s = compiler.visit_insert(insert, **kw)
        if 'append_string' in insert.kwargs:
            return s + " " + insert.kwargs['append_string']
        return s

    def load_district_table(self, district_df):
        # TODO testing
        listToWrite = district_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=self.engine)
        table = sqlalchemy.Table('District', metadata, autoload=True)

        self.conn.execute(table.insert(append_string='ON DUPLICATE KEY UPDATE coordinates = VALUES(coordinates)'), listToWrite)    
   
    
    def load_project_table(self, project_df):
        # TODO testing
        listToWrite = project_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=self.engine)
        table = sqlalchemy.Table('Project', metadata, autoload=True)

        self.conn.execute(table.insert(append_string='ON DUPLICATE KEY UPDATE district_id = VALUES(district_id)'), listToWrite)

    def load_property_table(self, property_df):
        listToWrite = property_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=self.engine)
        table = sqlalchemy.Table('Property', metadata, autoload=True)

        return self.conn.execute(table.insert(), listToWrite)

    def load_transaction_table(self, transaction_df):
        listToWrite = transaction_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=self.engine)
        table = sqlalchemy.Table('Transaction', metadata, autoload=True)

        self.conn.execute(table.insert(), listToWrite)

    def load_amenity_table(self, amenity_df):
        listToWrite = amenity_df.to_dict(orient='records')
        metadata = sqlalchemy.schema.MetaData(bind=self.engine)
        table = sqlalchemy.Table('Amenity', metadata, autoload=True)

        self.conn.execute(table.insert(append_string='ON DUPLICATE KEY UPDATE district_id = VALUES(district_id)'), listToWrite)

if __name__ == "__main__":
    db = UpdateDB(db_connect_type="IAM")
    # db.create_tables(CREATE_TABLES_SQL_PATH)