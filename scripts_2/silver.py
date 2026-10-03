from sparkSession import spark
from pyspark.sql.functions import *

## reading from the delta lake of bronze layer
dataframe = spark.read.format('delta').load('/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/bronze')

# Handling Nulls
## checking null counts across columns