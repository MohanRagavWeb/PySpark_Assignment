import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment3") \
    .master("local[*]") \
    .getOrCreate()

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

schemas = StructType([StructField("log id", IntegerType(), True),
                      StructField("user$id", IntegerType(), True),
                      StructField("action", StringType(), True),
                      StructField("timestamp", StringType(), True)])

df = spark.createDataFrame(data, schemas)
df.show()

#Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
for col_name in df.columns:
    new_col_name = col_name.replace(" ", "_").replace("$", "_").replace("action",
                             "user_activity").replace("timestamp","time_stamp")
    df = df.withColumnRenamed(col_name, new_col_name)
df.show(truncate=False)

#Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
df = df.withColumn("time_stamp", to_timestamp(col("time_stamp"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("login_date", to_date(col("time_stamp")))
df = df.drop("time_stamp")
df.show(truncate=False)

#calculate the number of actions performed by each user in the last 7 days
from pyspark.sql.functions import date_sub, max as spark_max
max_date = df.select(spark_max("login_date")).collect()[0][0]
df_last_7_days = df.filter(col("login_date") >= date_sub(col("login_date"), 7))
action_count = df_last_7_days.groupBy("user_id").agg(count("user_activity").alias("action_count"))
action_count.show()

#Write the data frame as a CSV file with different write options except (merge condition)
df.write.mode("overwrite").option("header", True).csv("dbfs:/FileStore/output/login_data_overwrite")
df.write.mode("append").option("header", True).csv("dbfs:/FileStore/output/login_data_append")
df.write.mode("overwrite").option("compression", "gzip").csv("dbfs:/FileStore/output/login_data_gzip")
df.write.mode("ignore").option("header", True).csv("dbfs:/FileStore/output/login_data_ignore")

#Write it as a managed table with the Database name as user and table name as login_details with overwrite mode.
spark.sql("CREATE DATABASE IF NOT EXISTS user")
df.write.mode("overwrite").saveAsTable("user.login_details")