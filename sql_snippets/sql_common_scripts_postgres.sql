-- Show all the tables in the database
SELECT *
FROM pg_catalog.pg_tables
WHERE 1=1
    AND schemaname != 'pg_catalog'
        AND schemaname != 'information_schema';


-- Show all the tables names in a given schema 
SELECT DISTINCT tablename 
FROM pg_catalog.pg_tables 
WHERE 1=1
    AND schemaname = 'public';

-- Show all the tables names in a given schema 
SELECT DISTINCT * 
FROM pg_catalog.pg_tables
WHERE 1=1
    AND schemaname != 'information_schema'
ORDER BY 2;


-- Show all the columns and related information in a given table
SELECT 
   table_name, 
   column_name, 
   ordinal_position,
   udt_name AS data_type,
   data_type AS data_type_desc,
   udt_catalog,
   udt_schema
   
FROM 
   information_schema.columns
WHERE 
   table_name = 'table_name';


-- Select rowds from a  given table 

SELECT 
    * 
FROM table_name 
LIMIT 1;