#%%
import os
import sys
sys.path.append(os.getcwd)

#%%
path_files = "../DBT_PROJECTS/project_dbt/models/core/dimensionss/"
files_to_change = os.listdir(path_files)
files_to_change

# %%
pattern = """
'dim_
""".strip()

new_pattern="""
'stg_
""".strip()


# %%
# %%
for filename in files_to_change:

    assert os.path.exists(path_files+'/'+filename)

    # Read in the file
    with open(path_files+'/'+filename, 'r') as file :
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace(pattern, new_pattern)

    # Write the file out again
    with open(path_files+'/'+filename, 'w') as file:
        file.write(filedata)
# %%
