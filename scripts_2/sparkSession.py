
from pyspark.sql import SparkSession
import os
import sys
from delta import configure_spark_with_delta_pip

# Forces both worker nodes and driver to use the exact virtual environment Python path
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession


builder = SparkSession.builder.appName('Spark Streaming session')\
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
        .config("spark.databricks.delta.identityColumn.enabled", "true")\
        .config("spark.sql.warehouse.dir", "./spark-warehouse")



# This helper function handles the "spark.jars.packages" string dynamically 
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

