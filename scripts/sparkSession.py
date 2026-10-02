
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('Spark Streaming session')\
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
        .getOrCreate()
    


