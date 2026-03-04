# driver.py
from pyspark.sql import SparkSession
from util import flatten_json, explode_array, camel_to_snake_case, add_load_date_columns
from pyspark.sql.functions import col

# -------------------------------
# 1️⃣ Create SparkSession
# -------------------------------
spark = SparkSession.builder.appName("JSON_Flatten_Explode").getOrCreate()

# -------------------------------
# 2️⃣ Read JSON
# -------------------------------
df = spark.read.option("multiLine", True).json(r"C:\Users\sheeb\Downloads\nested_json_file.json")

# -------------------------------
# 3️⃣ Flatten 'properties' struct
# -------------------------------
flattened_df = flatten_json(df, struct_columns=["properties"])

# -------------------------------
# 4️⃣ Explode employees array
# -------------------------------
exploded_df = explode_array(flattened_df, "employees", pos=True)

# Access employee details
exploded_df.select(
    "id",
    "name",
    "storeSize",
    "pos",
    "employees_item.empId",
    "employees_item.empName"
).show(truncate=False)

# -------------------------------
# 5️⃣ Filter id = 1001
# -------------------------------
filtered_df = flattened_df.filter(col("id") == 1001)

# -------------------------------
# 6️⃣ Convert camelCase → snake_case
# -------------------------------
clean_df = camel_to_snake_case(flattened_df)

# -------------------------------
# 7️⃣ Add load_date, year, month, day
# -------------------------------
final_df = add_load_date_columns(clean_df)
final_df.show(truncate=False)