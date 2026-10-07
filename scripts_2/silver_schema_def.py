from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    BooleanType, TimestampType, DateType, DecimalType, ArrayType, DoubleType
)

# Source -> silver mapping (from the generator)
#   clickstream: event_id, user_id, session_id, event_time, event_type, product_id,
#                category, device, country, price, quantity
#   users feed : user_id, email, country, loyalty_tier, signup_date, updated_at, change_type

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, DateType, DecimalType,
    IntegerType, LongType, BooleanType,
)

# Silver events: cleaned clickstream (nulls handled, deduped, types enforced)
fact_events_schema = StructType([
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


# Silver user: SCD2 dimension
dim_user_schema = StructType([
    StructField("user_sk",        LongType(),      False),  # surrogate key, unique per version
    StructField("user_id",        StringType(),    False),  # business key
    StructField("country",        StringType(),    True),  # tracked attribute
    StructField("effective_from", TimestampType(), False),  # = updated_at of the change
    StructField("effective_to",   TimestampType(), True),  # 9999-12-31 for the current row
    StructField("is_current",     BooleanType(),   True),
])


# Silver user: SCD2 dimension
dim_product_schema = StructType([
    StructField("product_sk",   LongType(),      nullable=False),
    StructField("product_id",   StringType(),    nullable=False),  # natural key
    StructField("category",     StringType(),    nullable=True),
    StructField("updated_ts",   TimestampType(), nullable=True),
])



silver_date_schema = StructType([
    StructField("date_key",     IntegerType(),   nullable=False),  
    StructField("full_date",    DateType(),      nullable=False),
    StructField("year",         IntegerType(),   nullable=True),
    StructField("quarter",      IntegerType(),   nullable=True),
    StructField("month",        IntegerType(),   nullable=True),
    StructField("monthname",    StringType(),   nullable=True),
    StructField("weekyear",     StringType(),   nullable=True),
    StructField("weekday",      StringType(),   nullable=True),
    StructField("dayname",      StringType(),   nullable=True),    
    StructField("day",          IntegerType(),   nullable=True),
    StructField("is_weekend",   BooleanType(),   nullable=True),
])
