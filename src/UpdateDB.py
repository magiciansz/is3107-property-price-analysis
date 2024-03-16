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

CREATE_TABLES_SQL_PATH = 'src/create_tables_clean.sql'

class UpdateDB:
    def __init__(self):
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
                    self.conn.execute(query)

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

    def update_transaction_table(self, transaction_df):
        pass







# TESTING ##########
if __name__ == '__main__':
    test = UpdateDB()
    # create_table_sql = """
    #     CREATE TABLE `District` (
    #         `district_id` int NOT NULL ,
    #         `district_name` varchar(50) NOT NULL ,
    #         `coordinates` char ,
    #         PRIMARY KEY (
    #             `district_id`
    #         )
    #     );
    #   """
    test.create_tables(CREATE_TABLES_SQL_PATH)