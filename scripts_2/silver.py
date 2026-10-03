from sparkSession import spark
from pyspark.sql.functions import *
from silver_schema_def import schema_silver_events, schema_silver_products, schema_silver_quarantined, schema_silver_users

## readpaths 
READ_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_clickstream'
READ_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_user'

## tables for silver layer schema 
##1. silver_events
##2. silver_events_quarantine
##3. silver_dim_customer
##4. silver_dim_products

## reading from the delta lake of bronze layer
dataframe_clickstream = spark.read.format('delta').load(READ_PATH_CLICKSTREAM)
dataframe_user = spark.read.format('delta').load(READ_PATH_USER)

## trim everything everywhere
df_cs_trimmed = dataframe_clickstream.withColumns({c: trim(col(c)) for c in dataframe_clickstream.columns})

## changing the schema of these dataframes since brone has all string type and no nullable constraints
df_cs = spark.createDataFrame(dataframe_clickstream.rdd, schema=schema_silver_events)

