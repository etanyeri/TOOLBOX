#%%
import sys
import os 

sys.path.append(os.getcwd)
#%%
path_files = "/Users/et/Desktop/TEST_REPLACE/REPLACE_FILE"
files_to_change = os.listdir(path_files)
files_to_change

pattern = """
'stg_
""".strip()

new_pattern= """
'dim_
""".strip()

for filename in files_to_change:

    #assert os.path.exists(path_files+'/'+filename)

    # Read in the file
    with open(path_files+'/'+filename, 'r') as file :
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace(pattern, new_pattern)

    # Write the file out again
    with open(path_files+'/'+filename, 'w') as file:
        file.write(filedata)

#%%
