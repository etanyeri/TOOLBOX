# Data Pipeline: File Path
#  {myOutputS3Loc}/table1.csv


#  Initial load
Select
	id
	,foriegnkey1_id
	,datetime1
	,datetime2
	,status
	,name
	,description
FROM
	table1
WHERE
	datetime1 <= '2021-04-25 23:59:59'


#  Incremental load
Select
	id
	,foriegnkey1_id
	,datetime1
	,datetime2
	,status
	,name
	,description
FROM
	table1
WHERE
	datetime1 >= '2021-04-26 00:00:00'
