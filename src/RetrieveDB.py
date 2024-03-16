import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

user = os.environ['LOCAL_USER']
password=os.environ['LOCAL_PW']
host= os.environ['LOCAL_HOST']
port = os.environ['LOCAL_PORT']
database=os.environ['LOCAL_DB']

class RetrieveDB:
    def __init__(self) -> None:
        def get_connection():
            # connect local db for testing
            return sqlalchemy.create_engine(
                url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(
                    user, password, host, port, database
                )
            )
            # return sqlalchemy.create_engine('mysql+pymysql://{user}:{password}@localhost/{database}')
        try:
            # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
            self.conn = get_connection().connect()
            print(
                f"Connection to the {host} for user {user} created successfully with {self.conn}.")
        except Exception as ex:
            print("Connection could not be made due to the following error: \n", ex)


    def get_project_id(self, project_name):
        query = sqlalchemy.text("""
                                SELECT project_id 
                                FROM Project
                                WHERE project_name = :{};
                               """.format(project_name))
        proj_id = self.conn.execute(query)
        return proj_id
    
    def get_property_id(self, project_id, property_type, street, lease_year, lease_duration, floor_range_start, floor_range_end, floor_area):
        # is there a more efficient way than this
        query = sqlalchemy.text("""
                                SELECT property_id
                                FROM Property
                                WHERE 
                                ...
                                

                                """)
        pass