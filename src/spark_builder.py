#session builder
import os
os.environ["SPARK_VERSION"] = "3.5"
from pyspark.sql import SparkSession

def get_spark(app_name='product_dim_scd2'):
    return (
        SparkSession.builder.appName(app_name).getOrCreate()
    )
