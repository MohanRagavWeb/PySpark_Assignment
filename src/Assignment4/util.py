# util.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, posexplode, current_date, year, month, dayofmonth
import re


# -------------------------------
# Flatten a JSON DataFrame
# -------------------------------
def flatten_json(df: DataFrame, struct_columns: list) -> DataFrame:
    """
    Flatten struct columns in a DataFrame
    :param df: Input DataFrame
    :param struct_columns: List of struct columns to flatten
    :return: Flattened DataFrame
    """
    for struct_col in struct_columns:
        for field in df.schema[struct_col].dataType.fields:
            df = df.withColumn(f"{field.name}", col(f"{struct_col}.{field.name}"))
        df = df.drop(struct_col)
    return df


# -------------------------------
# Explode array column
# -------------------------------
def explode_array(df: DataFrame, array_col: str, pos=False) -> DataFrame:
    """
    Explode an array column into rows
    :param df: Input DataFrame
    :param array_col: Array column name
    :param pos: If True, use posexplode
    :return: DataFrame with exploded rows
    """
    if pos:
        return df.selectExpr("*", f"posexplode({array_col}) as (pos, {array_col}_item)")
    else:
        return df.withColumn(f"{array_col}_item", explode(col(array_col)))


# -------------------------------
# CamelCase → snake_case
# -------------------------------
def camel_to_snake_case(df: DataFrame) -> DataFrame:
    def camel_to_snake(name):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    for c in df.columns:
        df = df.withColumnRenamed(c, camel_to_snake(c))
    return df


# -------------------------------
# Add load_date and year/month/day
# -------------------------------
def add_load_date_columns(df: DataFrame) -> DataFrame:
    df = df.withColumn("load_date", current_date())
    df = df.withColumn("year", year("load_date"))
    df = df.withColumn("month", month("load_date"))
    df = df.withColumn("day", dayofmonth("load_date"))
    return df