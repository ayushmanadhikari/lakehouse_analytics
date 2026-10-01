from sparkSession import spark
from pyspark.sql.types import StringType, StructField, IntegerType, StructType, DataType, DatetimeType, TimestampType, DoubleType


## defining json's structures
json_schema = StructType([
                StructField('event_id', StringType(), False),
                StructField('user_id', StringType(), False),
                StructField('session_id', StringType(), False),
                StructField('event_time', TimestampType(), True),
                StructField('event_type', StringType(), True),
                StructField('product_id', StringType(), True),
                StructField('category', StringType(), True),
                StructField('device', StringType(), True),
                StructField('country', StringType(), True),
                StructField('price', DoubleType(), True),
                StructField('quantity', IntegerType(), True)
])

## read the json files in data/streaming
file_path = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/data/streaming/'
dataframe = spark.readStream.json(file_path, schema=json_schema)


df = dataframe.writeStream.format('console').outputMode('append').start()







