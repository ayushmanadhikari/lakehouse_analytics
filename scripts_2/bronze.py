from sparkSession import spark
from pyspark.sql.functions import *

READ_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/data/streaming'
READ_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/data/users'

WRITE_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_clickstream'
WRITE_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_user'



## reading the json file into a dataframe
dataframe_c = spark.read.json(path=READ_PATH_CLICKSTREAM)
dataframe_user = spark.read.json(path=READ_PATH_USER)

dataframe_c.printSchema()

## adding data ingestion timestamp along with filename
dataframe_c = dataframe_c.withColumn('ingestion_ts', current_timestamp()).withColumn('ip_file_name', input_file_name())
dataframe_user = dataframe_user.withColumn('ingestion_ts', current_timestamp()).withColumn('ip_file_name', input_file_name())
#dataframe_c.show(2)


## storing into deltalake
print('writing to delta lake...')

dataframe_c.write.format('delta').mode('overwrite').option('overwriteSchema', True).save(WRITE_PATH_CLICKSTREAM)
dataframe_user.write.format('delta').mode('overwrite').option('overwriteSchema', True).save(WRITE_PATH_USER)

print('write complete!')