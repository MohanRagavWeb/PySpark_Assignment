# test.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType


# -------------------------------
# Flatten helper function
# -------------------------------
def flatten_json(df, struct_columns):
    for struct_col in struct_columns:
        for field in df.schema[struct_col].dataType.fields:
            df = df.withColumn(f"{field.name}", col(f"{struct_col}.{field.name}"))
        df = df.drop(struct_col)
    return df


# -------------------------------
# Pytest fixture
# -------------------------------
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()


# -------------------------------
# Single test case: flatten JSON
# -------------------------------
def test_flatten_json(spark):
    # Explicit schema to avoid MapType issue
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ]), True)
    ])

    data = [(1, {"name": "ABC", "storeSize": "Medium"})]
    df = spark.createDataFrame(data, schema=schema)

    df_flat = flatten_json(df, struct_columns=["properties"])

    assert "name" in df_flat.columns
    assert "storeSize" in df_flat.columns
    assert "properties" not in df_flat.columns