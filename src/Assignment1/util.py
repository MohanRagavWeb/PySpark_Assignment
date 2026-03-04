from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def customers_only_iphone13(purchase_df: DataFrame) -> DataFrame:
    """
    Returns customers who bought only iphone13.
    """
    return purchase_df.filter(F.col("product_model") == "iphone13") \
        .select("customer", "product_model")

def customers_upgraded_iphone13_to_iphone14(purchase_df: DataFrame) -> DataFrame:
    """
    Returns customers who bought both iphone13 and iphone14.
    """
    return (purchase_df.groupBy("customer")
            .agg(F.collect_set("product_model").alias("products"))
            .filter(F.array_contains(F.col("products"), "iphone13"))
            .filter(F.array_contains(F.col("products"), "iphone14"))
            .select("customer"))

def customers_bought_all_models(purchase_df: DataFrame, product_df: DataFrame) -> DataFrame:
    """
    Returns customers who bought all products in product_df.
    """
    total_products = product_df.count()
    customer_models = (purchase_df.select("customer", "product_model")
                       .distinct()
                       .groupBy("customer")
                       .agg(F.countDistinct("product_model").alias("model_count")))
    return customer_models.filter(F.col("model_count") == total_products).select("customer")