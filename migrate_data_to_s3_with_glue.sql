import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

aws_region = "<your-aws-region-code>"
s3_path = "<your-s3-prefix>"
glue_database = "<your-glue-database-name>"
target_format = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client = boto3.client(service_name='glue', region_name=aws_region)
responseGetTables = client.get_tables(DatabaseName=glue_database)

tableList = responseGetTables['TableList']
tables = []
for tableDict in tableList:
  tables.append(tableDict['Name'])

for table in tables:
  datasource = glueContext.create_dynamic_frame.from_catalog(database = glue_database, table_name = table)
  datasink = glueContext.write_dynamic_frame.from_options(frame = datasource, connection_type = "s3", connection_options = {"path": s3Path + table}, format = target_format)

job.commit()
