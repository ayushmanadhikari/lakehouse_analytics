from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('Spark Session').getOrCreate()