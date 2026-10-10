from sparkSession import spark
from pyspark.sql.functions import *


## reference path for each table of silver layer
DELTA_TABLE_PATH_EVENT = 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_event'
DELTA_TABLE_PATH_USER = 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_user'
DELTA_TABLE_PATH_DATE = 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_date'
DELTA_TABLE_PATH_PRODUCT = 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_product'

## write mode central control
WRITE_MODE = 'append'

## error encoutered: 
## We ran the spark_schema_def script for DDL to create all dimension and fact tables of the silver layer. This registered the delta table in the spark-catalog since we could easily reference it in the same session.
## But the problem arose when we tried to access that same delta table in another session.
## This was due to the fact that spark-catalog looses its memory across sessions and running a new session shows no registered catalogs. 
## so we needed to pass location in the create table syntax so that the spark-catalog registeres the tables found in that location before creating them.
## spark session recognizes the database folder(.db), but it has no idea that the silver_user folder is actually an official Delta table.


## writes the user dataframe into delta table silver_user
def write_silver_user(dataframe):
    # creates database if it doesn't exists. Since it recognized the db folder, maybe due to extension(.db). But it definitely fails to recognize the silver_user folder as a Delta Table.
    spark.sql("""CREATE DATABASE IF NOT EXISTS silver;""")
    # points the location to an already existing delta table
    spark.sql("""CREATE TABLE IF NOT EXISTS silver.silver_user 
    USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_user'
    """)
    ## we need to ensure idempotency so that same records donot get added.
    ## we will use left_anti join with existing data table 
    df_user = spark.read.format('delta').load(DELTA_TABLE_PATH_USER)
    dataframe = dataframe.join(df_user, on='user_id', how='left_anti')
    try:
        # actual write statement
        dataframe.write.format('delta').mode(WRITE_MODE).saveAsTable('silver.silver_user')
        spark.sql(""" select * from silver.silver_user""").show(2)
    except Exception as e:
        print(f"error occurred: {e}")


## writes the user dataframe into delta table silver_product
def wriite_silver_product(dataframe):
    spark.sql("""CREATE DATABASE IF NOT EXISTS silver;""")
    spark.sql("""CREATE TABLE IF NOT EXISTS silver.silver_product
        USING DELTA
        LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_product'
        """)
    ## we need to ensure idempotency so that same records donot get added.
    ## we will use left_anti join with existing data table 
    df_product = spark.read.format('delta').load(DELTA_TABLE_PATH_PRODUCT)
    dataframe = dataframe.join(df_product, on='user_id', how='left_anti')
    try:
        #actual write operation
        dataframe.write.format('delta').mode(WRITE_MODE).saveAsTable('silver.silver_product')
        spark.sql(""" select * from silver.silver_product""").show(2)
    except Exception as e:
        print(f"error: {e}")


## writes the user dataframe into delta table silver_date
def write_silver_date(dataframe):
    spark.sql("""CREATE DATABASE IF NOT EXISTS silver;""")
    spark.sql("""CREATE TABLE IF NOT EXISTS silver.silver_date
    USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_date' 
    """)
    ## we will use left_anti join with existing data table 
    df_date = spark.read.format('delta').load(DELTA_TABLE_PATH_DATE)
    dataframe = dataframe.join(df_date, on='user_id', how='left_anti')
    try: 
        dataframe.write.format('delta').mode(WRITE_MODE).saveAsTable('silver.silver_date')
        spark.sql(""" select * from silver.silver_date""").show(2)
    except Exception as e:
        print(f"error: {e}")


## writes the user dataframe into delta table silver_event
def write_silver_event(dataframe):
    spark.sql("""CREATE DATABASE IF NOT EXISTS silver;""")
    spark.sql("""CREATE TABLE IF NOT EXISTS silver.silver_event
    USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_event'
    """)
    ## we will use left_anti join with existing data table 
    df_event = spark.read.format('delta').load(DELTA_TABLE_PATH_EVENT)
    dataframe = dataframe.join(df_event, on='user_id', how='left_anti')
    try:
        dataframe.write.format('delta').mode(WRITE_MODE).saveAsTable('silver.silver_event')
        spark.sql(""" select * from silver.silver_event""").show(2)
    except Exception as e:
        print(f"error: {e}")