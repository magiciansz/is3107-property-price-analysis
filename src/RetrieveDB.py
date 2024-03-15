import pandas as pd
import numpy as np
import os
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy


class RetrieveDB:
    def __init__(self) -> None:
        self.conn

        pass

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