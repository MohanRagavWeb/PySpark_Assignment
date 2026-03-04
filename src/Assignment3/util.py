from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import col, to_date, current_timestamp, expr


def create_custom_schema():
    return StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])


def rename_columns(df, rename_map):
    for old_col, new_col in rename_map.items():
        df = df.withColumnRenamed(old_col, new_col)
    return df


def transform_dataframe(df):
    # Convert timestamp string to timestamp type
    df = df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

    # Filter actions in the last 7 days
    df_recent = df.filter(col("time_stamp") >= expr("current_timestamp() - interval 7 days"))

    # Count actions per user
    action_counts = df_recent.groupBy("user_id").count().withColumnRenamed("count", "action_count")

    # Add login_date column
    df = df.withColumn("login_date", to_date(col("time_stamp"), "yyyy-MM-dd")).withColumn("login_date",
                                                                                          col("login_date").cast(
                                                                                              DateType()))

    return df, action_counts