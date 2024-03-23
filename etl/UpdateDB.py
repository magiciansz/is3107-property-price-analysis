import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymysql
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert


from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv())

user = os.environ['MYSQL_USERNAME']
password=os.environ['MYSQL_PASSWORD']
host= os.environ['MYSQL_HOST']
port = os.environ['MYSQL_PORT']
database=os.environ['MYSQL_DATABASE_NAME']

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
            self.engine = get_connection()
            self.conn = self.engine.connect()
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


