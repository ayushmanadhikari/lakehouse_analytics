from sparkSession import spark
import sparkSession
import shutil

print(sparkSession.__file__)
print("spark:", spark.version)
print("packages:", spark.sparkContext.getConf().get("spark.jars.packages", None))
print("ext:", spark.conf.get("spark.sql.extensions", None))
print("cat:", spark.conf.get("spark.sql.catalog.spark_catalog", None))

BASE = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver'

PATH_EVENTS  = f'{BASE}/silver_fact_events'
PATH_USER    = f'{BASE}/silver_dim_user'
PATH_PRODUCT = f'{BASE}/silver_dim_product'
PATH_DATE    = f'{BASE}/silver_dim_date'


#################################

spark.sql("""CREATE DATABASE IF NOT EXISTS silver;""")
### use silver allows us to refer to the database directly without using database.table_name in the create table commands
spark.sql("""USE silver;""")

## create user table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_user (
    user_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    user_id string NOT NULL, 
    country string, 
    effective_from DATE,
    effective_to TIMESTAMP,
    is_current BOOLEAN
    ) USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_user'
""")

## create product table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_product (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    product_id STRING NOT NULL,
    category STRING,
    updated_ts TIMESTAMP
    ) USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_product'
""")

## create date table
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver_date (
    date_key BIGINT GENERATED ALWAYS AS IDENTITY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    monthname STRING,
    weekyear INT,
    weekday INT,
    dayname STRING,
    day INT,
    is_weekend BOOLEAN
    ) USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_date'
""")

## create event table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS silver_events (

    ) USING DELTA
    LOCATION 'file:/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_events'
""")


spark.sql("""
describe silver_user;
""").show()

spark.sql("""
select * from silver_user;
""").show()


#shutil.rmtree('spark-warehouse')