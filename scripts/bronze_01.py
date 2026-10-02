## outputs the datafrmae in parquet format, no delta lake layer
from sparkSession import spark
from pyspark.sql.types import StringType, StructField, IntegerType, StructType, DataType, DatetimeType, TimestampType, DoubleType
from pyspark.sql import functions as F

import shutil



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

## adding 2 columns, input filename and ingestion timestamp to the dataframe
dataframe = dataframe.withColumn('ip_filename', F.input_file_name()).withColumn('ingestion_ts', F.current_timestamp())

## write stream to write into a parquet file format
query = dataframe.writeStream.format('parquet').outputMode('append')\
        .option('checkpointLocation', './spark_checkpoints')\
        .option('path', 'parquet_op')\
        .trigger(availableNow=True).start()

query.awaitTermination()


## removing the existing checkpoints
shutil.rmtree('./spark_checkpoints', ignore_errors=True)
