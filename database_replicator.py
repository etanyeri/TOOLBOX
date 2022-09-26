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
# Import the credentials for the database or uncomment credentials variables below 
# credentials ={
#     "connection_name_here":{
#         "username": "postgres",
#         "password": "",
#         "host":"localhost",
#         "port":5433,
#         "database":"databasenamehere",
#         "drivername":"postgresql"
#     },
# }
#-------------------------------------------------------------------------------------------------------------------
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

 
#%%
#-------------------------------------------------------------------------------------------------------------------
#Establish and test the SOURCE database connection
#-------------------------------------------------------------------------------------------------------------------
#Update the connection_name variable if needed
connection_name_source="development" #str(input('Enter the SOURCE connection name from the credentials variable: '))
schema_source="development_ingested_schema" #str(input('Enter the SOURCE schema  name you want to connect: '))

engine_source = connect_database(connection_name=connection_name_source, schema=schema_source)

#Test Connection
query_test_connection=f"SELECT 'Connection to Postgres {connection_name_source}.{schema_source} is Successfull' AS Connection_Status, NOW() AS TIMESTAMP "
pd.read_sql_query(sql=query_test_connection,con=engine_source) 

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
#-------------------------------------------------------------------------------------------------------------------
# import the table names 
#-------------------------------------------------------------------------------------------------------------------

#Option 1:
#_________
# tables_to_replicate = pd.read_csv('../data/inputs/tablesnames.csv', dtype='object')['table_names']
# tables_to_replicate
#-------------------------------------------------------------------------------------------------------------------

#Option 2:
#________
# from variables.phoenix_analytics import table_names_rein_redshift as tables_to_replicate
#-------------------------------------------------------------------------------------------------------------------

#Option 3:
#_________
#SAVE SOURCE TABLENAMES TO A VARIABLE: EX: POSTGRES 
#-------------------------------------------------------------------------------------------------------------------
#Update the connection_name variable if needed
connection_name_pg="frontend-dev" 
schema_pg="public" 

engine_pg = connect_database(connection_name=connection_name_pg, schema=schema_pg)
#Identify the table names in the default schema
query_tablenames=f"""
SELECT DISTINCT tablename AS table_names
FROM pg_catalog.pg_tables 
WHERE 1=1
    AND schemaname = '{schema_pg}'
ORDER BY tablename ASC;
"""
tables_to_replicate = list(pd.read_sql_query(sql=query_tablenames,con=engine_pg)['table_names'])
print("Here are the tables in the schema:\n")
tables_to_replicate


#%%

# row count / table
i = 1 
for source_table_name in list(tables_to_replicate)[4:]:
    try:
        
        #-------------------------------------------------------------------------------------------------------------------
        #READ the tables FROM the SOURCE DB
        #-------------------------------------------------------------------------------------------------------------------
        # print(query)
        new_table_name = source_table_name.replace('frontend_public_','') 
        # print(new_table_name)
        query = f"Select * from {schema_source}.frontend_public_{source_table_name}"
  

        #___UNCOMMENT BELOW TWO LINES IF THE DATA FILES NEED TO BE REFRESHED____
        # df_source = pd.read_sql_query(sql=query,con=engine_source, dtype='object').fillna('')
        # df_source.to_csv(f'../data/outputs/phoenix_ingested_schema_tables_exports/{new_table_name}.csv', index=False)

        # print ( f"{i}. Saved {new_table_name} table")

        #-------------------------------------------------------------------------------------------------------------------
        #WRITE the tables TO the DESTINATION DB
        #-------------------------------------------------------------------------------------------------------------------
        df_source = pd.read_csv(f'../data/outputs/ingested_schema_tables_exports/{new_table_name}.csv')
        df_source.to_sql(con=engine_destination, name=new_table_name, if_exists='replace', schema='raw', low_memory=False)
    
        print(f'Uploaded  {new_table_name} table')
        time.sleep(1)
    except:
        print(f'{i}. ERROR: {new_table_name} did not upload')
        # print(f'{i}. ERROR: {new_table_name} did not download')

    i+=1
    

# %%
new_table_name = "users"
query = f"Truncate table {schema_destination}.{new_table_name} cascade"
engine_destination.execute(statement=query)
df_source = pd.read_csv(f'../data/outputs/ingested_schema_tables_exports/{new_table_name}.csv')
df_source.to_sql(con=engine_destination, name=new_table_name, if_exists='append', schema='raw')
# %%
