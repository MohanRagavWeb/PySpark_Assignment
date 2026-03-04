import os
import sys
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("Spark Assignment") \
    .master("local[*]") \
    .getOrCreate()

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
purchase_data_df = spark.createDataFrame(data=purchase, schema=schema)
#purchase_data_df.show()

product = [("iphone13",),
           ("dell i5 core",),
           ("dell i3 core",),
           ("hp i5 core",),
           ("iphone14",)]
schemas = StructType([StructField("product_model", StringType(), True)])
product_data_df = spark.createDataFrame(data=product, schema=schemas)
#product_data_df.show()


purchase_data_df.filter(purchase_data_df.product_model == "iphone13").show()
df1 = purchase_data_df.filter(purchase_data_df.product_model == "iphone13").select("customer").distinct()
df2 = purchase_data_df.filter(purchase_data_df.product_model == "iphone14").select("customer").distinct()
df3 = df1.join(df2, "customer","inner")
df4 = df1.intersect(df2)
df3.show()
df4.show()


total = product_data_df.count()
customer = (purchase_data_df.select("customer", "product_model")
    .distinct().groupBy("customer")
    .agg(F.countDistinct("product_model").alias("model_count")))
customers_all_models = customer.filter(F.col("model_count") == total)
customers_all_models.show()