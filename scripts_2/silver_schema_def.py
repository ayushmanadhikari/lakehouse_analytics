from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    BooleanType, TimestampType, DateType, DecimalType, ArrayType
)

# Source -> silver mapping (from the generator)
#   clickstream: event_id, user_id, session_id, event_time, event_type, product_id,
#                category, device, country, price, quantity
#   users feed : user_id, email, country, loyalty_tier, signup_date, updated_at, change_type

schema_silver_events = StructType([
    StructField('event_id', StringType(), False),
    StructField('session_id', StringType(), False),
    StructField('user_id', StringType(), False),          # null user_id rows are dropped
    StructField('user_sk', LongType(), False),            # point-in-time join to silver_users
    StructField('product_id', StringType(), False),       # null product_id rows are dropped
    StructField('product_sk', LongType(), False),
    StructField('event_ts', TimestampType(), False),      # from event_time; future-dated rows removed
    StructField('event_date', DateType(), False),
    StructField('event_type', StringType(), False),       # trimmed + lowercased
    StructField('category', StringType(), False),         # nulls backfilled from products / "unknown"
    StructField('unit_price', DecimalType(10, 2)),        # from price
    StructField('quantity', IntegerType()),               # null only for page_view / product_view
    StructField('device', StringType(), False),           # trimmed + lowercased, nulls -> "unknown"
    StructField('country', StringType(), False),          # nulls -> "unknown"
    StructField('ingest_ts', TimestampType(), False),
    StructField('dq_flags', ArrayType(StringType())),     # e.g. device_filled, country_filled
    StructField('_source_file', StringType()),
    StructField('_batch_id', StringType()),
    StructField('_processed_at', TimestampType(), False)
])

schema_silver_quarantined = StructType([
    StructField('quarantine_id', StringType(), False),
    StructField('source_table', StringType(), False),
    StructField('record_key', StringType()),              # event_id (or user_id for users feed)
    StructField('raw_record', StringType(), False),
    StructField('rule_name', StringType(), False),        # null_user_or_product, invalid_quantity, future_event_time
    StructField('reason', StringType()),
    StructField('event_ts_raw', StringType()),
    StructField('reprocessed', BooleanType(), False),
    StructField('_source_file', StringType()),
    StructField('_batch_id', StringType()),
    StructField('quarantined_at', TimestampType(), False),
    StructField('quarantine_date', DateType(), False)
])

# SCD2 tracked attributes: country, loyalty_tier
schema_silver_users = StructType([
    StructField('user_sk', LongType(), False),
    StructField('user_id', StringType(), False),
    StructField('email_hash', StringType()),              # sha256 of email
    StructField('email_domain', StringType()),
    StructField('signup_date', DateType()),
    StructField('country', StringType(), False),          # tracked
    StructField('loyalty_tier', StringType(), False),     # tracked
    StructField('valid_from', TimestampType(), False),    # = updated_at of this version
    StructField('valid_to', TimestampType(), False),      # next version's updated_at, or 9999-12-31
    StructField('is_current', BooleanType(), False),
    StructField('row_hash', StringType(), False),         # hash of country + loyalty_tier
    StructField('source_updated_at', TimestampType()),
    StructField('_batch_id', StringType()),
    StructField('_processed_at', TimestampType(), False)
])