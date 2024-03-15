import pandas as pd
import numpy as np
import os
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
import pymysql

from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

class UpdateDB:
    def __init__(self) -> None:
        self.conn = pymysql.connect(user = os.environ['LOCAL_USER'],
                               password=os.environ['LOCAL_PW'],
                               host= os.environ['LOCAL_HOSTNAME'],
                               database=os.environ['LOCAL_DB'])
        self.pool = pymysql.create_engine(
            "mysql+pymysql://",
            creator = self.conn,
        )
        self.pool.connect()
        
    def update_district_table(self, district_df):
        return
    
    
    def update_project_table(self, project_df):
        # TODO testing
        project_cols = ['district_id', 'project_name', 'long', 'lat']
        query = sqlalchemy.text("""
                                INSERT INTO Project(
                                district_id,
                                project_name,
                                long,
                                lat
                                )
                                VALUES (:{}, :{}, :{}, :{})
                                ON DUPLICATE KEY UPDATE 
                                district_id = VALUES(district_id),
                                long = VALUES(long),
                                lat = VALUES(lat)
                                """.format(project_cols))
        # self.conn.execute(query, project_df)
        # self.conn.commit()
        pass

    def update_project_property(self, project_property_df):
        project_cols = ['district_id', 'project_name', 'long', 'lat']
        property_cols = ['property_id', 'project_id', 'property_type', 'street', 'lease_year', 'lease_duration', 'floor_range_start', 'floor_range_end', 'floor_area']    
        query_insert_project = sqlalchemy.text("""
                                INSERT INTO Project(
                                district_id,
                                project_name,
                                long,
                                lat
                                )
                                VALUES (:{}, :{}, :{}, :{}) AS new
                                ON DUPLICATE KEY UPDATE 
                                district_id = new.district_id,
                                long = new.long,
                                lat = new.lat;
                                """.format(project_cols))  

        query_insert_property = sqlalchemy.text("""
                                                INSERT INTO Property(
                                                property_id ,
                                                project_id ,
                                                property_type,
                                                street,
                                                lease_year,
                                                lease_duration,
                                                floor_range_start,
                                                floor_range_end,
                                                floor_area,
                                                )
                                                VALUES (:{}, :{}, :{}, :{}, :{}, :{}, :{}, :{}, :{}) AS new
                                                ON DUPLICATE KEY UPDATE
                                                ....
                                                
                                                """.format(property_cols))
        for row in project_property_df:
            self.conn.execute(query_insert_project, row[project_cols])
            query_get_project_id = sqlalchemy.text("""SELECT LAST_INSERT_ID();""")
            
            # TODO check whether this is doable!
            # https://stackoverflow.com/questions/42964049/query-to-auto-increment-id-in-parent-table-after-insert-new-row-in-child-table
            # https://mariadb.com/kb/en/auto_increment-on-insert-on-duplicate-key-update/

            # alternatively query project_id using project_name as the latter is required to be unique

            project_id =  self.conn.execute(query_get_project_id) 
            row['project_id'] = project_id
            self.conn.execute(query_insert_property, row[property_cols])

        pass