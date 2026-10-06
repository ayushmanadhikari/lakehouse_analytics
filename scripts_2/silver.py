from sparkSession import spark
from pyspark.sql.functions import *
from silver_schema_def import fact_events_schema, dim_user_schema, dim_product_schema, silver_date_schema
from pyspark.sql.types import *

## readpaths 
READ_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_clickstream'
READ_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_user'

## tables for silver layer schema 
##1. silver_events
##2. silver_dim_customer
    #- user_id 
##3. silver_dim_products

## reading from the delta lake of bronze layer
dataframe_clickstream = spark.read.format('delta').load(READ_PATH_CLICKSTREAM)
dataframe_user = spark.read.format('delta').load(READ_PATH_USER)


# uses dictionary comprehension to trim and lower case every string in the dataframe
def trim_everything_lower(dataframe):
    df_trimmed = dataframe.withColumns({c: lower(trim(col(c))) for c in dataframe.columns})
    return df_trimmed


## ensures unique value on event id to ensure no duplicate clickstream entries
def ensure_unique(dataframe):
    df_unique = dataframe.dropDuplicates(subset=['event_id'])
    return df_unique


## filter future dates in event_time
def filter_future_events(dataframe):
    df_ts = dataframe.withColumn('event_time', upper(trim(col('event_time'))))
    #df_ts = df_ts.withColumn('event_time', to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))
    df_ts = df_ts.filter(col('event_time') <= current_timestamp())
    return df_ts


## user_id rows, null product_id, (quantity rows for event_type purchase, add_to_cart and checkout)
def handle_nulls(dataframe):
    # checking null counts accross all columns
    dataframe.withColumns({c: sum(col(c).isNull().cast('int')) for c in dataframe_clickstream.columns}).show()
    # replaces with 'unknown' value for columns - category, country, device
    df = dataframe.fillna('unknown', subset=['category', 'country', 'device'])
    # dropping for user_id, event_time, session_id
    df = df.dropna(how='any', subset=['user_id', 'event_time', 'session_id'])
    ## handle NULLs, negatives and 0 in quantity as per event_type
    df = df.filter(~((col('event_type').isin('add_to_cart', 'purchase', 'checkout')) & (col('quantity') <= 0) & (col('quantity').isNull())))
    ## handling null product per event_type
    df = df.filter(~((col('event_type').isin('add_to_cart', 'purchase', 'checkout')) & (col('product_id').isNull())))
    # checking null counts accross all columns
    df.withColumns({c: sum(col(c).isNull().cast('int')) for c in dataframe_clickstream.columns}).show()
    # filters negative priced products
    df = df.filter(~((col('price').try_cast('double'))<=0))
    return df


## converts the schema type of all string schema to specfic types
def type_conversion(dataframe):
    ## this is done to ensure that no nulls value remain in these columns before converting them to nullable=False property
    ## even if the null count is 0, it is safer to use coalesce anyways before changing nullable property of a column
    df = dataframe.withColumns({'user_id': coalesce('user_id', lit('unkonwn')),
                'event_id': coalesce('event_id', lit('unkonwn')),
                'event_time': coalesce('event_time', lit('unkonwn')),
                'session_id': coalesce('session_id', lit('unknown'))
                })
    df = df.withColumn('event_time', to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

    try:
        df = df.to(schema=fact_events_schema)      #does the actualy conversion but do not forget to coalesce to change nullable before passing the StructType object to (.to()) method or else it will break
        return df
    except Exception as e:
        print(f'error: {e}')



def main():
    df = trim_everything_lower(dataframe_clickstream)
    df = ensure_unique(df)
    df = handle_nulls(dataframe_clickstream)
    df = filter_future_events(df)
    df.printSchema()
    df = type_conversion(df)
    df.printSchema()


if __name__ == '__main__':
    main()