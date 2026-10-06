from sparkSession import spark

# creates schema silver
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# creates dim_user
query = """
CREATE TABLE IF NOT EXISTS silver.dim_user(
    user_sk BIGINT GENERATED AS 
) USING DELTA

"""

