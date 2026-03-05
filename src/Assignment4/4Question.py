import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import re
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment4") \
    .master("local[*]") \
    .getOrCreate()

#Read JSON file provided in the attachment using the dynamic function
df = spark.read.option("multiLine", True).json(r"C:\Users\mohan\Downloads\nested_json_file.json")
df.show(truncate=False)
df.printSchema()

# Flatten manually (example)
flattened_df = df.select(
    col("id"),
    col("properties.name").alias("name"),
    col("properties.storeSize").alias("store_size"),
    col("employees")
)
flattened_df.show(truncate=False)
flattened_df.printSchema()

#Record count: flattened vs non-flattened
original_count = df.count()       # top-level objects
flattened_count = flattened_df.count()  # still 1 because array not exploded
print(f"Original count: {original_count}")
print(f"Flattened count: {flattened_count}")

#Explode employees array
# Regular explode
df_exploded = flattened_df.withColumn("employee", explode("employees"))
df_exploded.show(truncate=False)
# explode_outer keeps nulls
df_exploded_outer = flattened_df.withColumn("employee_outer", explode_outer("employees"))
df_exploded_outer.show(truncate=False)
# posexplode gives position
df_pos_exploded = flattened_df.selectExpr(
    "id",
    "name",
    "store_size",
    "posexplode(employees) as (pos, employee)"
)
df_pos_exploded.show(truncate=False)

# Access employee details
df_pos_exploded.select(
    "id",
    "name",
    "store_size",
    "pos",
    "employee.empId",
    "employee.empName"
).show(truncate=False)


#Filter id = 1001
filtered_df = flattened_df.filter(col("id") == 1001)
filtered_df.show(truncate=False)

#Convert camelCase → snake_case
def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
for c in flattened_df.columns:
    flattened_df = flattened_df.withColumnRenamed(c, camel_to_snake(c))
flattened_df.show(truncate=False)

#Add load_date column
flattened_df = flattened_df.withColumn("load_date", current_date())
flattened_df.show(truncate=False)

#Extract year, month, day from load_date
flattened_df = flattened_df \
    .withColumn("year", year("load_date")) \
    .withColumn("month", month("load_date")) \
    .withColumn("day", dayofmonth("load_date"))
flattened_df.show(truncate=False)