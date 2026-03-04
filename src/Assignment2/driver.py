# driver.py
import os
import sys
from pyspark.sql import SparkSession
from util import mask_card_udf

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():
    spark = SparkSession.builder \
        .appName("Spark Assignment2") \
        .master("local[*]") \
        .getOrCreate()

    # Sample credit card data
    card = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    # Create DataFrame
    credit_card_df = spark.createDataFrame(card, ["card_number"])
    credit_card_df.show(truncate=False)

    # Print original partitions
    print("Original number of partitions:", credit_card_df.rdd.getNumPartitions())

    # Repartition to 5
    credit_card_df = credit_card_df.repartition(5)
    print("Number of partitions after repartition:", credit_card_df.rdd.getNumPartitions())

    # Coalesce back to original (if needed)
    original_partitions = 12  # If you want to restore original, store manually
    credit_card_df = credit_card_df.coalesce(original_partitions)
    print("Number of partitions after coalesce:", credit_card_df.rdd.getNumPartitions())

    # Apply UDF
    masked_df = credit_card_df.withColumn("masked_card_number", mask_card_udf("card_number"))
    masked_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()