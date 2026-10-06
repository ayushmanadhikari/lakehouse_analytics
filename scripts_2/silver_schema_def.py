from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    BooleanType, TimestampType, DateType, DecimalType, ArrayType
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
schema_silver_events = StructType([
    StructField("category",     StringType(),       False),  # backfilled / "unknown"
    StructField("country",      StringType(),       False),  # backfilled / "unknown"
    StructField("device",       StringType(),       False),  # trimmed + lowercased / "unknown"
    StructField("event_id",     StringType(),       False),  # unique after dedupe
    StructField("event_time",   TimestampType(),    False),  # parsed, future dates removed
    StructField("event_type",   StringType(),       False),  # trimmed + lowercased
    StructField("price",        DecimalType(10, 2), False),
    StructField("product_id",   StringType(),       False),  # null rows dropped
    StructField("quantity",     IntegerType(),      True),   # null only for page_view / product_view
    StructField("session_id",   StringType(),       False),
    StructField("user_id",      StringType(),       False),  # null rows dropped
    StructField("ingestion_ts",  TimestampType(),    False),  # silver load time
    StructField("ip_file_name",  StringType(),       True),   # lineage from bronze
])


# Silver user: SCD2 dimension
schema_silver_user = StructType([
    StructField("user_sk",        LongType(),      False),  # surrogate key, unique per version
    StructField("user_id",        StringType(),    False),  # business key
    StructField("email",          StringType(),    True),
    StructField("signup_date",    DateType(),      True),
    StructField("country",        StringType(),    False),  # tracked attribute
    StructField("loyalty_tier",   StringType(),    False),  # tracked attribute
    StructField("effective_from", TimestampType(), False),  # = updated_at of the change
    StructField("effective_to",   TimestampType(), False),  # 9999-12-31 for the current row
    StructField("is_current",     BooleanType(),   False),
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
