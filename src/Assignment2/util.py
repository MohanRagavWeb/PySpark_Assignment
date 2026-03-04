# util.py
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def mask_card(card_number):
    """
    Mask all but last 4 digits of a credit card number.
    """
    return '*' * (len(card_number)-4) + card_number[-4:]

# UDF to use in DataFrame transformations
mask_card_udf = udf(mask_card, StringType())