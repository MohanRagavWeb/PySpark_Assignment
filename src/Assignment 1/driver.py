import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from util import customers_only_iphone13, customers_upgraded_iphone13_to_iphone14, customers_bought_all_models

# Spark session
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment") \
    .master("local[*]") \
    .getOrCreate()

# ------------------ Data ------------------
purchase = [(1, "iphone13"),
 (1, "dell i5 core"),
 (2, "iphone13"),
 (2, "dell i5 core"),
 (3, "iphone13"),
 (3, "dell i5 core"),
 (1, "dell i3 core"),
 (1, "hp i5 core"),
 (1, "iphone14"),
 (3, "iphone14"),
 (4, "iphone13")]

schema = StructType([StructField("customer", IntegerType(), True),
                    StructField("product_model", StringType(), True)])
purchase_df = spark.createDataFrame(data=purchase, schema=schema)

product = [("iphone13",),
           ("dell i5 core",),
           ("dell i3 core",),
           ("hp i5 core",),
           ("iphone14",)]
product_schema = StructType([StructField("product_model", StringType(), True)])
product_df = spark.createDataFrame(data=product, schema=product_schema)

# ------------------ Operations ------------------
print("Customers who bought only iphone13:")
customers_only_iphone13(purchase_df).show()

print("Customers upgraded from iphone13 to iphone14:")
customers_upgraded_iphone13_to_iphone14(purchase_df).show()

print("Customers who bought all products:")
customers_bought_all_models(purchase_df, product_df).show()