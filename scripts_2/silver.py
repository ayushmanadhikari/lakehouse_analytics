from sparkSession import spark
from pyspark.sql.functions import *
from silver_schema_def import fact_events_schema, dim_user_schema, dim_product_schema, silver_date_schema
from pyspark.sql.types import *
from pyspark.sql import Window

## readpaths 
READ_PATH_CLICKSTREAM = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_clickstream'
READ_PATH_USER = '/Users/ayusman/thisWorks/DE/lakehouse_analytics/lakehouse/bronze/bronze_user'



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
    ## even if the null count is 0, it is safer to use coalesce anyways before changing nullable property of a column
    df = dataframe.withColumns({'user_id': coalesce('user_id', lit('unkonwn')),
                'event_id': coalesce('event_id', lit('unkonwn')),
                'event_time': coalesce('event_time', lit('unkonwn')),
                'session_id': coalesce('session_id', lit('unknown'))
                })
    df = df.withColumn('event_time', to_timestamp(col('event_time'), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

    try:
        df = df.to(schema=events_schema)      #does the actualy conversion but do not forget to coalesce to change nullable before passing the StructType object to (.to()) method or else it will break
        return df
    except Exception as e:
        print(f'error: {e}')


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

    try:
        df_latest_user = df_latest_user.to(user_schema)
    except Exception as e:
        raise('error: '+ str(e))
    return df_latest_user


def create_date_df(dataframe):
    df_time = dataframe.withColumn('event_time', to_timestamp('event_time', "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))
    df_time = df_time.select('event_time')
    df_time = df_time.withColumns({
        'full_date': to_date('event_time', 'yyyy-MM-dd'),
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
    return df_time

def create_product_df(dataframe):
    # creates product_df to be inserted into dim_product
    pass 

silver_date_schema = StructType([
    StructField("date_key",     IntegerType(),   nullable=False),  # yyyyMMdd
    StructField("full_date",    DateType(),      nullable=False),
    StructField("year",         IntegerType(),   nullable=False),
    StructField("quarter",      IntegerType(),   nullable=False),
    StructField("month",        IntegerType(),   nullable=False),
    StructField("monthname",    StringType(),   nullable=False),
    StructField("week",         StringType(),   nullable=False),
    StructField("weekday",         StringType(),   nullable=False),
    StructField("day",          IntegerType(),   nullable=False),
    StructField("is_weekend",   BooleanType(),   nullable=True),
])

def main():
    '''df = trim_everything_lower(dataframe_clickstream)
    df = ensure_unique(df, 'event_id')
    df = handle_nulls(dataframe_clickstream)
    df = filter_future_events(df)
    df.printSchema()
    df = type_conversion(df)
    df.printSchema() '''
    #df = create_user_df(dataframe_user)
    df = create_date_df(dataframe_clickstream)
    df.printSchema()
    df.show(5, truncate=False)


if __name__ == '__main__':
    main()