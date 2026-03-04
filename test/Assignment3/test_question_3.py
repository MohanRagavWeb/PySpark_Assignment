from src.Assignment3.Question3 import df

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    from pyspark.sql.functions import col, to_date, to_timestamp, lit, expr


    def test_transform():
        spark = SparkSession.builder.appName("TestCustomSchema").getOrCreate()

        data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]

        schema = StructType([
            StructField("log id", IntegerType(), True),
            StructField("user$id", IntegerType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])

        df = spark.createDataFrame(data, schema=schema)

        rename_map = {
            "log id": "log_id",
            "user$id": "user_id",
            "action": "user_activity",
            "timestamp": "time_stamp"
        }
        for old_col, new_col in rename_map.items():
            df = df.withColumnRenamed(old_col, new_col)

        df = df.withColumn("time_stamp", col("time_stamp").cast("timestamp"))

        # Use fixed reference date for testing
        reference_date = to_timestamp(lit("2023-09-13 00:00:00"))
        df_recent = df.filter(col("time_stamp") >= reference_date - expr("INTERVAL 7 DAYS"))

        action_counts = df_recent.groupBy("user_id").count().withColumnRenamed("count", "action_count")

        df = df.withColumn("login_date", to_date(col("time_stamp")))

        print("Test DataFrame with login_date:")
        df.show(truncate=False)

        print("Test Action Counts in Last 7 Days:")
        action_counts.show()

        spark.stop()


    if __name__ == "__main__":
        test_transform()
    df.show()
    action_counts.show()
except Exception as e:
    print("Test failed with error:")
    print(str(e).split('\n')[0])  # Show only the first line of the error