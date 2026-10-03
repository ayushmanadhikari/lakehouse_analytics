from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    BooleanType, TimestampType, DateType, DecimalType, ArrayType
)

schema_silver_events = StructType([
    StructField('event_id', StringType(), False),
    StructField('session_id', StringType(), False),
    StructField('user_id', StringType()),
    StructField('user_sk', LongType(), False),
    StructField('product_id', StringType()),
    StructField('product_sk', LongType()),
    StructField('event_ts', TimestampType(), False),
    StructField('event_date', DateType(), False),
    StructField('event_type', StringType(), False),
    StructField('is_logged_in', BooleanType(), False),
    StructField('is_bot', BooleanType(), False),
    StructField('category', StringType()),
    StructField('unit_price', DecimalType(10, 2)),
    StructField('currency', StringType(), False),
    StructField('quantity', IntegerType()),
    StructField('order_id', StringType()),
    StructField('search_query', StringType()),
    StructField('device', StringType(), False),
    StructField('country', StringType(), False),
    StructField('traffic_source', StringType()),
    StructField('app_version', StringType()),
    StructField('user_agent', StringType()),
    StructField('ip_hash', StringType()),
    StructField('ingest_ts', TimestampType(), False),
    StructField('dq_flags', ArrayType(StringType())),
    StructField('_source_file', StringType()),
    StructField('_batch_id', StringType()),
    StructField('_processed_at', TimestampType(), False)
])

schema_silver_products = StructType([
    StructField('product_sk', LongType(), False),
    StructField('product_id', StringType(), False),
    StructField('product_name', StringType()),
    StructField('category', StringType(), False),
    StructField('brand', StringType()),
    StructField('unit_price', DecimalType(10, 2), False),
    StructField('currency', StringType(), False),
    StructField('valid_from', TimestampType(), False),
    StructField('valid_to', TimestampType(), False),
    StructField('is_current', BooleanType(), False),
    StructField('row_hash', StringType(), False),
    StructField('_batch_id', StringType()),
    StructField('_processed_at', TimestampType(), False)
])

schema_silver_quarantined = StructType([
    StructField('quarantine_id', StringType(), False),
    StructField('source_table', StringType(), False),
    StructField('record_key', StringType()),
    StructField('raw_record', StringType(), False),
    StructField('rule_name', StringType(), False),
    StructField('reason', StringType()),
    StructField('event_ts_raw', StringType()),
    StructField('reprocessed', BooleanType(), False),
    StructField('_source_file', StringType()),
    StructField('_batch_id', StringType()),
    StructField('quarantined_at', TimestampType(), False),
    StructField('quarantine_date', DateType(), False)
])

schema_silver_users = StructType([
    StructField('user_sk', LongType(), False),
    StructField('user_id', StringType(), False),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email_hash', StringType()),
    StructField('email_domain', StringType()),
    StructField('country', StringType(), False),
    StructField('city', StringType()),
    StructField('loyalty_tier', StringType(), False),
    StructField('marketing_opt_in', BooleanType()),
    StructField('valid_from', TimestampType(), False),
    StructField('valid_to', TimestampType(), False),
    StructField('is_current', BooleanType(), False),
    StructField('is_deleted', BooleanType(), False),
    StructField('row_hash', StringType(), False),
    StructField('source_updated_at', TimestampType()),
    StructField('_batch_id', StringType()),
    StructField('_processed_at', TimestampType(), False)
])