#%%
import os
import sys
sys.path.append(os.getcwd)

#%%
path_files = "../DBT_PROJECTS/aw_dbt/models/staging/dimensions/"
files_to_change = os.listdir(path_files)
files_to_change


# %%
for file_name in files_to_change:

    file_exists = os.path.exists(path_files+file_name)
    if file_exists:
        new_file_name = file_name.replace('dim_', 'stg_')
        os.rename(path_files+file_name, path_files+new_file_name)
   
# %%
