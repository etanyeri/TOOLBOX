
#%%
import os
import sys
sys.path.append(os.getcwd)
current_directory = os.getcwd()
home_directory = current_directory+'/..'
sys.path.append(home_directory)
#%%
from distutils.command.config import config
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy.dialects.mysql import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy import orm
from sqlalchemy_redshift.dialect import TIMESTAMPTZ, TIMETZ
from configs.credentials import credentials

import pandas as pd

#%%
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
#SAVE SOURCE TABLENAMES TO A VARIABLE: EX: POSTGRES 
#-------------------------------------------------------------------------------------------------------------------
#Update the connection_name variable if needed
connection_name_postgresfrontend="re-frontend-dev" #str(input('Enter the SOURCE connection name from the credentials variable: '))
schema_postgresfrontend="public" #str(input('Enter the SOURCE schema  name you want to connect: '))
engine_pg = connect_database(connection_name=connection_name_postgresfrontend, schema=schema_postgresfrontend)
#Identify the table names in the default schema
query_tablenames=f"""
SELECT DISTINCT tablename AS table_names
FROM pg_catalog.pg_tables 
WHERE 1=1
    AND schemaname = '{schema_postgresfrontend}'
ORDER BY tablename ASC;
"""

tables_to_replicate = list(pd.read_sql_query(sql=query_tablenames,con=engine_pg)['table_names'])
print("Here are the tables in the schema:\n")
tables_to_replicate

#%%
path_files = "../DBT_PROJECTS/project_dbt/models/staging/rein/facts/"
assert os.path.exists(path_files)

#%%
#save a generic pattern
pattern = """
--{{ config(enabled=True) }}

-- choose dimension
WITH fct_source AS (
    SELECT * FROM {{ source( 'raw', 'table_name' ) }}
),

final AS (
    SELECT * FROM fct_source
)

SELECT
    *
FROM final
""".strip()


# %%
for table_name in tables_to_replicate:
    # Read in the file
    with open(path_files+'stg_Rein_'+table_name+'.sql', 'w') as file :
        file.write(pattern)

# %%









