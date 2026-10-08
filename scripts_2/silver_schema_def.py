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
spark.sql("""USE silver;""")


spark.sql("""
CREATE TABLE IF NOT EXISTS silver_user (
user_sk BIGINT GENERATED ALWAYS AS IDENTITY,
user_id string NOT NULL, 
country string, 
effective_from DATE,
effective_to TIMESTAMP,
is_current BOOLEAN
) USING DELTA
""")

spark.sql("""
describe silver_user;
""").show()

spark.sql("""
select * from silver_user;
""").show()


#shutil.rmtree('spark-warehouse')