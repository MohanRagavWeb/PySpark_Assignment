from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def create_spark():
    spark = (
        SparkSession.builder
        .appName("EmployeeAssignment5")
        .master("local[1]")  # safer for Windows
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.python.worker.memory", "512m")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
    return spark

def create_dataframes(spark):
    dataset1 = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]
    dataset2 = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]
    dataset3 = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    emp_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
    ])
    dept_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    employee_df = spark.createDataFrame(dataset1, schema=emp_schema)
    department_df = spark.createDataFrame(dataset2, schema=dept_schema)
    country_df = spark.createDataFrame(dataset3, schema=country_schema)

    return employee_df, department_df, country_df

def average_salary(employee_df):
    return employee_df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))

def employee_name_starts_with_m(employee_df, department_df):
    return (
        employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
        .filter(F.col("employee_name").startswith("m"))
        .select("employee_name", "dept_name")
    )

def add_bonus_column(employee_df):
    return employee_df.withColumn("bonus", F.col("salary") * 2)

def reorder_columns(employee_df):
    return employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department", "bonus")

def join_departments(employee_df, department_df):
    inner_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    left_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
    right_df = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")
    return inner_df, left_df, right_df

def replace_state_with_country(employee_df, country_df):
    return (
        employee_df.join(country_df, employee_df.State == country_df.country_code, "left")
        .drop("State", "country_code")
        .withColumnRenamed("country_name", "State")
    )

def lowercase_and_add_load_date(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())
    df = df.withColumn("load_date", F.current_date())
    return df

def save_as_external_tables(df):
    df.write.mode("overwrite").option("header", True).csv("employee_csv_table")
    df.write.mode("overwrite").parquet("employee_parquet_table")