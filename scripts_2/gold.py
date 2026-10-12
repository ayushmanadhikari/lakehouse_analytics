from sparkSession import spark
from pyspark.sql.functions import *


READ_PATH_USER_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_user'
READ_PATH_DATE_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_date'
READ_PATH_PRODUCT_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_product'
READ_PATH_EVENT_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/silver.db/silver_events'


## joins the event table with every other dimension table. since dim tables are small, we are using broadcast
def join_dims(dataframe):
    df_product = spark.read.format('delta').load(READ_PATH_PRODUCT_SILVER)
    df_user = spark.read.format('delta').load(READ_PATH_USER_SILVER)
    df_date = spark.read.format('delta').load(READ_PATH_DATE_SILVER)
    dataframe = dataframe.join(broadcast(df_user), on='user_sk', how='inner')\
                        .join(broadcast(df_product), on='product_sk', how='inner')\
                        .join(broadcast(df_date), on='date_key', how='inner')
    return dataframe

def daily_kpis(dataframe):
    ## groupby event_date, event type, active users, sessions and total revenue
    df_kpis = dataframe.groupBy('event_date').agg(count_distinct('user_sk').alias('distinct users'), count_distinct('session_id').alias('sessions'), 
                round(sum('price'), 2).alias('total_revenue'))
    df_kpis.show(5)
    return df_kpis


## number of users per event_type and total revenue
def funnel_metrics(dataframe):
    df_fmetrics = dataframe.groupBy('event_type').agg(count('event_type').alias('total events'), count_distinct('user_sk').alias('total users')).orderBy('total events',  ascending=False)
    df_fmetrics.show(5)
    return df_fmetrics

## product_id, price and revenue
def product_metrics(dataframe):
    dataframe = join_dims(dataframe)
    df_pmetrics = dataframe.groupBy("product_id", "category").agg(count("*").alias("total_interactions"),
                    round(sum("price"), 2).alias("revenue")
                    ).orderBy(col("revenue").desc(), col("total_interactions").desc())
    df_pmetrics.show(5)
    return df_pmetrics

## top 50 users as per purchase amounts
def top_fifty(dataframe):
    dataframe = join_dims(dataframe)
    df_top_user = dataframe.filter(col('event_type') == 'purchase')\
                                .groupBy('user_id').agg(count_distinct('product_id').alias('product count'), 
                                round(sum('price'),2).alias('purchase totals'), count_distinct('event_id').alias('no. of transactions'))\
                                .orderBy('purchase totals', ascending=False)
    df_top_user.show(10)
    return df_top_user



def main():
    df_event = spark.read.format('delta').load(READ_PATH_EVENT_SILVER)
    df_kpis = daily_kpis(df_event)
    df_fmetrics = funnel_metrics(df_event)
    df_pmetrics = product_metrics(df_event)
    df_top_user = top_fifty(df_event)


if __name__ == '__main__':
    main()



'''
daily_kpis = (
    df.groupBy("event_date")
    .agg(
        count("*").alias("total_events"),
        countDistinct("user_id").alias("active_users"),
        countDistinct("session_id").alias("sessions"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue")
    )
)

funnel_metrics = (
    df.groupBy("event_type")
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    )
    .orderBy("event_count", ascending=False)
)

product_metrics = (
    df.groupBy("product_id", "category")
    .agg(
        count("*").alias("total_interactions"),
        spark_round(spark_sum("revenue"), 2).alias("revenue")
    )
    .orderBy(col("revenue").desc(), col("total_interactions").desc())
)
'''