import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

user = os.environ['MYSQL_USERNAME']
password=os.environ['MYSQL_PASSWORD']
host= os.environ['MYSQL_HOST']
port = os.environ['MYSQL_PORT']
database=os.environ['MYSQL_DATABASE_NAME']

class RetrieveDB:
    def __init__(self) -> None:
        def get_connection():
            # connect local db for testing
            return sqlalchemy.create_engine(
                url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                    user, password, host, port, database
                ),
            )
            # return sqlalchemy.create_engine('mysql+pymysql://{user}:{password}@localhost/{database}')
        try:
            # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
            self.engine = get_connection()
            self.conn = self.engine.connect()
            print(
                f"Connection to the {host} for user {user} created successfully with {self.conn}.")
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
    
    def get_last_transaction_id(self):
        query = sqlalchemy.text("""
                                SELECT max(id)
                                FROM Transaction
                               """)
        results = self.conn.execute(query)
        id = results.fetchone()[0]
        if not id:
            return 1
        return id