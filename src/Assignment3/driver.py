from pyspark.sql import SparkSession
from util import create_custom_schema, rename_columns, transform_dataframe

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CustomSchemaExample").getOrCreate()

    # Sample data
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

    # Step 1: Create DataFrame with custom schema
    schema = create_custom_schema()
    df = spark.createDataFrame(data, schema=schema)

    # Step 2: Rename columns dynamically
    rename_map = {
        "log id": "log_id",
        "user$id": "user_id",
        "action": "user_activity",
        "timestamp": "time_stamp"
    }
    df_renamed = rename_columns(df, rename_map)

    # Step 3 & 4: Filter last 7 days and add login_date
    df_transformed, action_counts = transform_dataframe(df_renamed)

    print("Transformed DataFrame:")
    df_transformed.show(truncate=False)

    print("Action Counts in Last 7 Days:")
    action_counts.show()

    spark.stop()