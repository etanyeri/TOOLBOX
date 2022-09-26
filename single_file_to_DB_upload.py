# %%
#-------------------------------------------------------------------------------------------------------------------
'''
REPLICATES A DATABASE SCHEMA:
    # provide the credentials in the config/profile.py file 
    #update the connection_name variables for source and destination database connections
-------------------------------------------------------------------------------------------------------------------
'''
#%%
import os
import sys
current_directory = os.getcwd()
home_directory = current_directory+'/..'
sys.path.append(home_directory)
from distutils.command.config import config

import csv
import pandas as pd
import ast
import numpy as np 
import time

from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.dialects.mysql import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import orm
from sqlalchemy_redshift.dialect import TIMESTAMPTZ, TIMETZ
from configs.credentials import credentials
#-------------------------------------------------------------------------------------------------------------------
#DEFINE CREDENTIALS
from configs.credentials import credentials
# %%
#-------------------------------------------------------------------------------------------------------------------
#DEFINE CUSTOM FUNCTIONS
#-------------------------------------------------------------------------------------------------------------------

def connect_database(connection_name, schema):
    '''
    This is function that establishes a connection to an RDBS with sqlAlchemy.

    Parameters:
        connection_info (dict):  "connection_name":{ "username": "", "password": "", "host":"", "connection_string": "", "port":int, "database":"", "drivername":"" }
        schema (str): Default 'public'

    Usage:
        engine = connect_database(connection_info=credentials['connection_name'],  schema='public')

    Returns:
        database connection engine
    '''
    from configs.credentials import credentials
    connection_info = credentials[connection_name]
    engine = create_engine(
        str(
            URL.create(
                drivername=connection_info['drivername'], 
                host=connection_info['host'], 
                password=connection_info['password'], 
                port=connection_info['port'], 
                username=connection_info['username'], 
                database=connection_info['database']
            )
        )
    )
    return engine


# %%
#-------------------------------------------------------------------------------------------------------------------
#Establish and test the DESTINATION database connection
#-------------------------------------------------------------------------------------------------------------------
#Update the connection_name variable if needed
connection_name_destination="localhost" #str(input('Enter the DESTINATION connection name from the credentials variable: '))
schema_destination="raw" #str(input('Enter the DESTINATION schema  name you want to connect: '))

engine_destination = connect_database(connection_name=connection_name_destination, schema=schema_destination)

#Test Connection
query_test_connection= f"SELECT 'Connection to local Postgres {connection_name_destination}.{schema_destination} is Successfull' AS Connection_Status, NOW() AS TIMESTAMP "
pd.read_sql_query(sql=query_test_connection,con=engine_destination)


#%%
file_path = f'../data/outputs/ingested_schema_tables_exports/active_comments.csv'
table_name = 'active_comments'
pd.read_csv(file_path)
#%%
try:
    #-------------------------------------------------------------------------------------------------------------------
    #WRITE the tables TO the DESTINATION DB
    #-------------------------------------------------------------------------------------------------------------------
    df_source = pd.read_csv(file_path)
    df_source.to_sql(con=engine_destination, name=table_name, if_exists='replace', schema=schema_destination)

    print(f'Uploaded  {table_name} table')
except:
    print(f'ERROR: {table_name} did not upload')

    
# %%
