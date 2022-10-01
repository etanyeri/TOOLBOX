#%%
from gettext import npgettext
import pandas as pd
import numpy as np

# sql engine
from sqlalchemy import create_engine
import cryptography
from  py_library.file_definitions import file_definitions

#You must import USER, KEY, HOST, DB_NAME from your config file.

engine_read = create_engine(
    f"mysql+pymysql://{USER}:{KEY}@{HOST}:3306/{DB_NAME}", echo=False
)

engine_write = create_engine(
    f"mysql+pymysql://{USER}:{KEY}@{HOST}:3306/{DB_NAME}", echo=False
)

################################33
#%%
file_info = {
            'id': 'some_number',
            'name': 'download_name',
            'data_source': 'anaplan',
            'spreadsheetID': 'example_additional_info',
            'model_name': 'any_project_name',
            'modelID': 'any_project_id',
            'source_table_name': 'table_to_write_into',
            'destination_table': 'table_to_write_into',
            'destination_folder': 'data/downloads/',
            'header_row': 0,
            'file_type': 'csv',
            'sep': ',',
            'dtype': 'string',
            'na_values': ['na', 'NA', '', 'NULL', '.', ' '],
            'columns_to_ignore':-1,# this is the starting index of the autogenerated columns which are usually the last two columns
            'minimum_no_rows':10,
            'etl_schedule_interval_hrs':1,
            'last_upload_time':0,
            'last_download_time':0,
            'export_headers': [
                'ID'
            ],
            'destination_table_headers':[
                'ID',
                'CURRENT_TIME'
            ]
        }




###
#DOWNLOAD THE FILE
def migrate(file_info, engine_read, engine_write ):

    #CONNECTING TO THE DATA SOURCE AND DOWNLOADING THE SOURCE DATA
    query= f"select * from {file_info['source_table_name']}"

    df = pd.read_sql( con=engine_read, query=query)

    df.to_sql( name=file_info["destination_table"],
                     con=engine_write, 
                     if_exists="append", 
                     index=False
    )
    return f"Uploaded {file_info['destination_table"]} successfully'

migrate(file_info, engine_read, engine_write )








#REST IS EXAMPLES

#%%

# RUN DATA QUALITY TESTS

data = (
    pd.read_csv(
        f'{os.getcwd()}/{file_info["destination_folder"]}{file_info["name"]}',
        dtype="object",
        header=file_info["header_row"],
        sep=file_info["sep"],
        na_values=file_info["na_values"],
        usecols=file_info["export_headers"],
    )
    .replace('"', "'")[file_info["export_headers"]]



# %%
# test #1: Compare column headers of the export to the expected headers to confirm export hasn't changed
assert (
    list(data.columns) == file_info["export_headers"]
), f"{file_info['name']} headers are different than expected export_headers. Possible solutions: a) Revert the changes in export definition. b) update the file_infos to reflect the changes."
# %%
# test #2: Compare current column headers of the destination table to the expected DB headers to confirm DB table hasn't changed
assert (
    colsDB == file_info["destination_table_headers"]
), f"{file_info['destination_table']} headers in DB are different than expected destination_table headers. Possible solutions:\n a) Revert the changes in DB.\n b) update the file_infos to reflect the changes."
# %%
# test #3: Confirm that the size of the table exceeds the expected threshold
row_count_data = len(data)
assert (
    row_count_data > file_info["minimum_no_rows"]
), f"Data size is too small to pass the minimum threshold. Suggested actions:\n a) Manually check the downloaded export and confirm the file size and format \n b) Check the download credentials"
# %%


#WRITE TO THE DESTINATION DB

# LOAD DATA to THE NEW DB
engine = create_engine(
    f"mysql+pymysql://{USER}:{KEY}@{HOST}:3306/{DB_NAME}", echo=False
)
try:
    data = (
        pd.read_csv(
            f'{path}/{file_info["destination_folder"]}{file_info["name"]}',
            dtype="object",
            header=file_info["header_row"],
            sep=file_info["sep"],
            na_values=file_info["na_values"],
            usecols=file_info["export_headers"],
        ).replace('"', "'")[file_info["export_headers"]]
    )
    data.columns = file_info["destination_table_headers"][
        : file_info["columns_to_ignore"]
    ]
    # truncate the table
    query_truncate = f"TRUNCATE TABLE {file_info['destination_table']}"
    engine.execute(query_truncate)
    # upload the data
    data.to_sql(
        name=file_info["destination_table"], con=engine, if_exists="append", index=False
    )
    print(" uploaded data")
except:
    print("error uploading data")