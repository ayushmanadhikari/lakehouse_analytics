from sparkSession import spark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import datetime
from datetime import datetime as dt
from lr_delta import *


## readpaths from bronze layer
READ_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_clickstream'
READ_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_user'

## readpaths from silver layer. needs these paths while joining with fact table to get the surrogate keys generated during insertion
READ_PATH_USER_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_user'
READ_PATH_DATE_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_date'
READ_PATH_PRODUCT_SILVER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/spark-warehouse/silver.db/silver_product'


DEFAULT_TIMESTAMP = dt(1970, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
DATE_DEFAULT = lit(datetime.date(1970, 1, 1))

## tables for silver layer schema 
##1. silver_events
##2. silver_dim_customer
    #- user_id 
##3. silver_dim_products

## reading from the delta lake of bronze layer events data and user data
dataframe_clickstream = spark.read.format('delta').load(READ_PATH_CLICKSTREAM)
dataframe_user = spark.read.format('delta').load(READ_PATH_USER)


## defining the final structure for events dataframe
events_schema = StructType([
    StructField('category', StringType(), nullable=True),
    StructField('country', StringType(), nullable=True),
    StructField('device', StringType(), nullable=True),
    StructField('event_id', StringType(), nullable=False),
    StructField('event_time', TimestampType(), nullable=False),
    StructField('event_date', DateType(), nullable=True),
    StructField('event_type', StringType(), nullable=True),
    StructField('price', DoubleType(), nullable=True),
    StructField('product_id', StringType(), nullable=True),
    StructField('quantity', IntegerType(), nullable=True),
    StructField('session_id', StringType(), nullable=False),
    StructField('user_id', StringType(), nullable=False),
    StructField('ingestion_ts', TimestampType(), nullable=True),
    StructField('ip_file_name', StringType(), nullable=True),
    ])

# uses dictionary comprehension to trim and lower case every string in the dataframe
def trim_everything_lower(dataframe):
    df_trimmed = dataframe.withColumns({c: lower(trim(col(c))) for c in dataframe.columns})
    return df_trimmed


## ensures unique value on event id to ensure no duplicate clickstream entries
def ensure_unique(dataframe, *fields):
    df_unique = dataframe.dropDuplicates(subset=fields)
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
    ## even if the null count is 0, it is safer to use coalesce anyways before changing nullable property of a column, with the same datatype lit as default value
    df = dataframe.withColumns({'user_id': coalesce('user_id', lit('unkonwn')),
                'event_id': coalesce('event_id', lit('unkonwn')),
                'event_time': coalesce('event_time', lit('unkonwn')),
                'event_date': coalesce(to_date(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"), lit(DATE_DEFAULT)),
                'session_id': coalesce('session_id', lit('unknown')),
                'price': coalesce('price', lit('unkown'))
                })
    df = df.withColumn('event_time', coalesce(to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"), lit(DEFAULT_TIMESTAMP)))
    df = df.withColumns({
        'price': col('price').cast('double'),
        'quantity': col('quantity').cast('int'),
        'ingestion_ts': col('ingestion_ts').cast('timestamp')
        })
    try:
        df = df.to(schema=events_schema)      #does the actualy conversion but do not forget to coalesce to change nullable before passing the StructType object to (.to()) method or else it will break
        return df
    except Exception as e:
        print(f'error: {e}')
        print('done')


def create_event_df(dataframe):
    df = trim_everything_lower(dataframe)
    df = ensure_unique(df, 'event_id')
    df = handle_nulls(df)
    df = filter_future_events(df)
    df = type_conversion(df)
    return df

## prearing the event_df by joining and applying foregin key constraints to make it write ready
## since we will be using the surrogate keys from the dimension tables, we cannot use left_semi, so we will use left join, select required columns only and use fallback to address any missing dimension entries
def create_event_table(df_event):
    df_user = spark.read.format('delta').load(READ_PATH_USER_SILVER)
    df_date = spark.read.format('delta').load(READ_PATH_DATE_SILVER)
    df_product = spark.read.format('delta').load(READ_PATH_PRODUCT_SILVER)

    df_user.show(5)
    print(df_user.count())
    df_date.show(5)
    print(df_date.count())
    df_product.show(5)
    print(df_product.count())

    
    # using left join to return even the non-matching rows and using coalesce on the surrogate key column to populate -1 in missing cases
    df_event = df_event.join(broadcast(df_user), on='user_id', how='left')\
                        .join(broadcast(df_date), df_event.event_date == df_date.full_date, how='left')\
                        .join(broadcast(df_product), on='product_id', how='left')
    df_event = df_event.withColumns({
        'user_sk': coalesce(col('user_sk'), lit('-1')),
        'date_key': coalesce(col('date_key'), lit('-1')),
        'product_sk': coalesce(col('product_sk'), lit('-1'))
    })
    df_event = df_event.select('event_id', 'event_time', 'event_date', 'event_type', 'session_id', 'device', 'price', 'quantity', 'ingestion_ts', 'ip_file_name', 'user_sk', 'product_sk', 'date_key')
    df_event.show(5)
    print(df_event.count())
    return df_event
    
    


def create_user_df(dataframe):
    df_user = trim_everything_lower(dataframe)
    # defining window spec
    window_spec_latest_user = Window.partitionBy('user_id').orderBy(col('updated_at').desc())
    ## using this window spec with rank function
    df_latest_user = df_user.withColumn('ranks', rank().over(window_spec_latest_user))
    ## adding is_current flag, so latest record show True and old ones false
    df_latest_user = df_latest_user.withColumn('is_current', when(col('ranks') == 1, True).otherwise(False))
    df_latest_user = df_latest_user.withColumnRenamed('signup_date', 'effective_from').withColumnRenamed('updated_at', 'effective_to')
    df_latest_user = df_latest_user.select('user_id', 'country', 'effective_from', 'effective_to', 'is_current')
    ## type conversion
    ## defining schema
    user_schema = StructType([
        StructField('user_id', StringType(), False),
        StructField('country', StringType(), True),
        StructField('effective_from', DateType(), False),
        StructField('effective_to', TimestampType(), True),
        StructField('is_current', BooleanType(), True)
    ])
    
    ## coalescing not-null fields for precaution without date/datetime casting
    df_latest_user = df_latest_user.withColumns({
        'user_id': coalesce(col('user_id'), lit('unknown')),
        'effective_from': coalesce(upper(col('effective_from')), lit('1970-01-01')),
        'effective_to': coalesce(upper(col('effective_to')), lit("9999-12-31 23:59:00"))
    })

    ##changing type for date columns
    df_latest_user = df_latest_user.withColumns({
        'effective_from': to_date('effective_from', 'yyyy-MM-dd'),
        'effective_to': to_timestamp('effective_to', "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
    })

    df_latest_user = df_latest_user.dropDuplicates(subset=['user_id', 'effective_from', 'effective_to'])

    try:
        df_latest_user = df_latest_user.to(user_schema)
    except Exception as e:
        raise('error: '+ str(e))
    return df_latest_user


### NOTE:
## converting a nullable = True field to nullable = False is a hassle when you also need to perform type casting from string to other type. 
# Properly follow the following steps:
# 1. typecasting: convert the string data type to the required data type first, in this case string(containing timestamp) is first converted into timestamp type then date type.
# Remember to provide the matching pattern of timestamp and date while type casting
#2. Coalesce with a default value of the same type so that catalyst otpimizer can evaluate that field to contain no null values
# while coalescing you must keep in mind to provide the default value to be the same datatype as the source column. You cannot use .cast("date") in this step.
# This is because, casting a string to any other type is marked as possibly nullable because the parse can fail. 
# Use a real Python date literal, which creates a non-null DateType literal directly:
def create_date_df(dataframe):
    silver_date_schema = StructType([
    StructField("full_date",    DateType(),      nullable=False),
    StructField("year",         IntegerType(),   nullable=True),
    StructField("quarter",      IntegerType(),   nullable=True),
    StructField("month",        IntegerType(),   nullable=True),
    StructField("monthname",    StringType(),   nullable=True),
    StructField("weekyear",     IntegerType(),   nullable=True),
    StructField("weekday",      StringType(),   nullable=True),
    StructField("dayname",      StringType(),   nullable=True),    
    StructField("day",          IntegerType(),   nullable=True),
    StructField("is_weekend",   BooleanType(),   nullable=False),
])
    # type coversion for full_Date to dateType from stringtype of eventtype(first to timestamp then to date)
    df_time = dataframe.select('event_time')
    df_time = df_time.withColumn('full_date', to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX").cast("date"))

    # since casting string to anyother type marks it as nullable, we will be using datetype object itself and then coalescing to drop nullable property
    
    df_time = df_time.withColumn('full_date', coalesce(col('full_date'), DATE_DEFAULT))
    
    # do not re type cast full_date into date type, this again allows nulls. so it would break nullable=False, which is True to this point
    df_time = df_time.withColumns({
        'year': year(to_date('full_date')),
        'quarter': quarter(to_date('full_date')),
        'month': month(to_date('full_date')),
        'monthname': monthname(to_date('full_date')),
        'weekyear': weekofyear(to_date('full_date')),
        'weekday': weekday(to_date('full_date')),
        'dayname': dayname(to_date('full_date')),
        'day': day(to_date('full_date')),
        'is_weekend': when(col('weekday').isin('5', '6'), True).otherwise(False),
    })

    df_time = df_time.select('full_date','year', 'quarter', 'month', 'monthname', 'weekyear', 'weekday', 'dayname', 'day', 'is_weekend').distinct()
    df_time = df_time.dropDuplicates(subset=['full_date'])
    df_time = df_time.to(silver_date_schema)
    return df_time


def create_product_df(dataframe):
    dim_product_schema = StructType([
    StructField("product_id",   StringType(),    nullable=False),  # natural key
    StructField("category",     StringType(),    nullable=True),
    StructField("updated_ts",   TimestampType(), nullable=False),
    ])

    df_product = dataframe.select('product_id', 'category', 'event_time').distinct()
    timestamp_default = lit(dt.now())

    df_product = df_product.withColumns({
        'product_id': coalesce(col('product_id'), lit('unknown')),
        'category': col('category'),
        'updated_ts': coalesce(to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"), timestamp_default)
    })

    ## since there are 11000 rows of product_df, we will drop many of these to keep only 4 rows per product
    ## using dense rank and only keeping ranks 1-4
    window_spec = Window.partitionBy('product_id').orderBy(col('updated_ts').asc())
    df_product_ranked = df_product.withColumns({'temp_rank': dense_rank().over(window_spec)})
    df_product_ranked = df_product_ranked.filter(col('temp_rank') < 5)
    print(df_product_ranked.count())
    
    df_product = df_product_ranked.select('product_id', 'category', 'event_time', 'updated_ts')
    df_product = df_product.dropDuplicates(subset=['product_id', 'updated_ts'])
    df_product = df_product.to(dim_product_schema)
    return df_product


def main():
    #df_event = create_event_df(dataframe_clickstream)
    df_user = create_user_df(dataframe_user)
    #df_date = create_date_df(dataframe_clickstream)
    #df_product = create_product_df(dataframe_clickstream)


    #df_event.show(3)
    df_user.show(3)
    df_user.printSchema()
    #df_date.show(3)
    #df_product.show(3)
    
    write_silver_event(df_event)
    write_silver_user(df_user)
    write_silver_date(df_date)
    wriite_silver_product(df_product)

    df_event = create_event_table(df_event)

if __name__ == '__main__':
    main()