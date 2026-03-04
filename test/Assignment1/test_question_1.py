import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


# ----------------------------
# Fixture: Spark Session Setup
# ----------------------------
@pytest.fixture(scope="session")
def spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder \
        .appName("Assignment one Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


# ----------------------------
# Fixture: Purchase DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def purchase_data_df(spark):
    data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]
    schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


# ----------------------------
# Fixture: Product DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def product_data_df(spark):
    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]
    schema = StructType([
        StructField("product_model", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


# ----------------------------
# Test 1: Customers who bought only iPhone13
# ----------------------------
def test_customers_bought_iphone13(purchase_data_df):
    result_df = (
        purchase_data_df
        .filter(f.col("product_model") == "iphone13")
        .select("customer")
        .distinct()
    )

    print("\nCustomers who bought iPhone13:")
    result_df.show()
    assert result_df.count() > 0


# ----------------------------
# Test 2: Customers who upgraded from iPhone13 → iPhone14
# ----------------------------
def test_customers_upgraded_iphone13_to_iphone14(purchase_data_df):
    df1 = purchase_data_df.filter(f.col("product_model") == "iphone13").select("customer").distinct()
    df2 = purchase_data_df.filter(f.col("product_model") == "iphone14").select("customer").distinct()

    upgraded_customers = df1.join(df2, "customer", "inner")

    print("\nCustomers who upgraded from iPhone13 to iPhone14:")
    upgraded_customers.show()
    assert upgraded_customers.count() > 0


# ----------------------------
# Test 3: Customers who bought all models
# ----------------------------
def test_customers_bought_all_models(purchase_data_df, product_data_df):
    total_models = product_data_df.count()

    df_count = (
        purchase_data_df
        .select("customer", "product_model")
        .distinct()
        .groupBy("customer")
        .agg(f.countDistinct("product_model").alias("model_count"))
    )

    customer_all_models = df_count.filter(f.col("model_count") == total_models)

    print("\nCustomers who bought all models in product data:")
    customer_all_models.show()

    customer_ids = [row.customer for row in customer_all_models.collect()]
    assert 1 in customer_ids
    assert customer_all_models.count() == 1