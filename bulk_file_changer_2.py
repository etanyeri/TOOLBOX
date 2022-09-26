
import sys
sys.path.append(os.getcwd)

#%%
path_files = "../DBT_PROJECTS/project_dbt/models/core/dimensions"
files_to_change = os.listdir(path_files)
files_to_change

# %%
pattern = """,

cte__convert_to_timestamp AS (
    SELECT
        *,
        {{convert_to_timestamp(column_name='created_at', default_if_null='1970-01-01', format_as='YYYY-MM-DD')}}        AS created_at_timestamp,
        {{convert_to_timestamp(column_name='discarded_at', default_if_null=-1, format_as='milliseconds')}}        AS discarded_at_timestamp
    FROM dim_source

),

cte__is_effective AS (
    SELECT
        *,
        (created_at_timestamp > discarded_at_timestamp) AS is_effective
    FROM cte__convert_to_timestamp
),

cte__dim_source_w_effective_tag AS (
    SELECT
        dim_source.*,
        cte__is_effective.is_effective
    FROM dim_source
    LEFT JOIN cte__is_effective ON dim_source.id = cte__is_effective.id
),

final AS (
    SELECT * FROM cte__dim_source_w_effective_tag
)

SELECT
    *
FROM final
""".strip()

replace_pattern = """
,
final AS (
    SELECT * FROM dim_source 
)
SELECT
    *
FROM final
""".strip()

#%%
file = open("test.sql")
# %%
# %%
for filename in files_to_change:
    # Read in the file
    with open(path_files+'/'+filename, 'r') as file :
        filedata = file.read()

    # Replace the target string
    filedata = filedata.replace(pattern, replace_pattern)

    # Write the file out again
    with open(filename, 'w') as file:
        file.write(filedata)
# %%
