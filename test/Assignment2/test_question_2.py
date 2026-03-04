import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *
from pyspark.sql.functions import udf


# ----------------------------
# Fixture: Spark Session Setup
# ----------------------------
@pytest.fixture(scope="session")
def spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder \
        .appName("Credit Card Masking Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


# ----------------------------
# Fixture: Credit Card DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def credit_card_df(spark):
    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]
    schema = StructType([StructField("card_number", StringType(), True)])
    return spark.createDataFrame(data, schema)


# ----------------------------
# Test 1: Check original number of partitions
# ----------------------------
def test_original_partitions(credit_card_df):
    original_partitions = credit_card_df.rdd.getNumPartitions()
    print(f"Original number of partitions: {original_partitions}")
    assert original_partitions > 0


# ----------------------------
# Test 2: Repartition to 5 and back (adjusted for Windows)
# ----------------------------
def test_repartition_coalesce(credit_card_df):
    original_partitions = credit_card_df.rdd.getNumPartitions()

    # Repartition to 5
    df_repart = credit_card_df.repartition(5)
    print(f"Partitions after repartition: {df_repart.rdd.getNumPartitions()}")
    assert df_repart.rdd.getNumPartitions() == 5

    # Coalesce back: only decreases partitions, cannot increase on Windows local mode
    target_partitions = min(original_partitions, df_repart.rdd.getNumPartitions())
    df_coalesce = df_repart.coalesce(target_partitions)
    print(f"Partitions after coalesce (adjusted): {df_coalesce.rdd.getNumPartitions()}")
    assert df_coalesce.rdd.getNumPartitions() <= original_partitions


# ----------------------------
# Test 3: Mask credit card numbers using UDF
# ----------------------------
def test_mask_card_udf(credit_card_df):
    # Define mask function and UDF
    def mask_card(card_number):
        return '*' * (len(card_number) - 4) + card_number[-4:]

    mask_card_udf = udf(mask_card, StringType())

    df_masked = credit_card_df.withColumn("masked_card_number", mask_card_udf("card_number"))
    df_masked.show(truncate=False)

    # Validate masking
    results = [row.masked_card_number for row in df_masked.collect()]
    for original, masked in zip([row.card_number for row in credit_card_df.collect()], results):
        assert masked.endswith(original[-4:])
        assert masked[:-4] == '*' * (len(original) - 4)


# ----------------------------
# Test 4: Test plain Python function mask_card
# ----------------------------
def test_mask_card_function():
    def mask_card(card_number):
        return '*' * (len(card_number) - 4) + card_number[-4:]

    assert mask_card("1234567891234567") == "************4567"
    assert mask_card("987654321098") == "********1098"