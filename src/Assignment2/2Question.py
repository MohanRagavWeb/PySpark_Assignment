import os
import sys

from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment2") \
    .master("local[*]") \
    .getOrCreate()

card = (("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",) )
credit_card_df = spark.createDataFrame(card, ["card_number"])
credit_card_df.show()

print("Original number of partitions:", credit_card_df.rdd.getNumPartitions())

credit_card_df = credit_card_df.repartition(5)
print("Number of partitions after repartition:", credit_card_df.rdd.getNumPartitions())

original_partitions = credit_card_df.rdd.getNumPartitions()  # store original number first
credit_card_df = credit_card_df.coalesce(original_partitions)
print("Number of partitions after coalesce:", credit_card_df.rdd.getNumPartitions())

def mask_card(card_number):
    return '*' * (len(card_number)-4) + card_number[-4:]
mask_card_udf = udf(mask_card, StringType())
masked_df = credit_card_df.withColumn("masked_card_number", mask_card_udf("card_number"))
masked_df.show(truncate=False)