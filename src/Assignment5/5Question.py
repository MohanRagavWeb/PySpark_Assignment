import os
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment5") \
    .master("local[*]") \
    .getOrCreate()

Dataset1 = ((11,"james"," D101","ny",9000,34),
(12,"michel"," D101","ny",8900,32),
(13,"robert"," D102","ca",7900,29),
(14,"scott"," D103","ca",8000,36),
(15,"jen"," D102","ny",9500,38),
(16,"jeff"," D103","uk",9100,35),
(17,"maria"," D101","ny",7900,40))

schema1 = StructType([StructField("employee_id", IntegerType(), True),
                      StructField("employee_name", StringType(), True),
                      StructField("dept_id", StringType(), True),
                      StructField("country_code", StringType(), True),
                      StructField("salary", IntegerType(), True),
                      StructField("Age", IntegerType(), True)])
employee_df = spark.createDataFrame(Dataset1, schema1)
employee_df.show()

Dataset2 = (("D101","sales"),
("D102","finance"),
("D103","marketing"),
("D104","hr"),
("D105","support"))

schema2 = StructType([StructField("dept_id", StringType(), True),
                      StructField("dept_name", StringType(), True)])
department_df = spark.createDataFrame(Dataset2, schema2)
department_df.show()

Dataset3 = (("ny","newyork"),
("ca","California"),
("uk","Russia"))

schema3 = StructType([StructField("country_code", StringType(), True),
                      StructField("country_name", StringType(), True)])
country_df = spark.createDataFrame(Dataset3, schema3)
country_df.show()

#Find avg salary of each department
employee_df.groupBy("dept_id").agg(avg("salary").alias("avg_salary")).show()

#Find the employee’s name and department name whose name starts with ‘m’
df = employee_df.join(department_df, "dept_id", "inner")
rdf = df.filter(col("employee_name").rlike("(?i)^m")).select("employee_name", "dept_name")
rdf.show()

#Create another new column in  employee_df as a bonus by multiplying employee salary *2
employee_df.withColumn("bonus", col("salary")*2).show()

#Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)
emp_df = employee_df.join(department_df, "dept_id", "left")
emp_df = emp_df.join(country_df, "country_code", "left") \
               .drop("country_code") \
               .withColumnRenamed("country_name", "State")
emp_df.show()

#Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way
def join_df(df1: DataFrame, df2: DataFrame, join_type: str = "inner", join_col: str = "dept_id"):
    return df1.join(df2, on=join_col, how=join_type)
# Inner join
inner_df = join_df(employee_df, department_df, join_type="inner")
inner_df.show()
# Left join
left_df = join_df(employee_df, department_df, join_type="left")
left_df.show()
# Right join
right_df = join_df(employee_df, department_df, join_type="right")
right_df.show()

# Join employee_df with country_df to get country_name
emp_with_country = employee_df.join(country_df, employee_df.country_code == country_df.country_code, "left") \
                              .drop(employee_df.country_code) \
                              .withColumnRenamed("country_name", "State")  # rename as 'State' if needed
emp_with_country.show()

#Convert all column names to lowercase dynamically + add load_date
emp_lower = emp_with_country.toDF(*[c.lower() for c in emp_with_country.columns])
emp_lower = emp_lower.withColumn("load_date", current_date())
emp_lower.show()

#create 2 external tables with parquet, CSV format with the
# same name database name, and 2 different table names as CSV and parquet format.
csv_path = "F:/pyspark_assignment/output/employee_csv"
parquet_path = "F:/pyspark_assignment/output/employee_parquet"
# Save as CSV
emp_lower.write.mode("overwrite").option("header", True).csv(csv_path)
# Save as Parquet
emp_lower.write.mode("overwrite").parquet(parquet_path)
spark.sql(f"CREATE DATABASE IF NOT EXISTS my_database")
# CSV table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS my_database.employee_csv
USING CSV
OPTIONS (
  path '{csv_path}',
  header 'true'
)
""")
# Parquet table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS my_database.employee_parquet
USING PARQUET
OPTIONS (
  path '{parquet_path}'
)
""")