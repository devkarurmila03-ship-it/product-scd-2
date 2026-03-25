#etl py
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit,to_date
from datetime import datetime

def read_daily_data(spark,bucket_uri:str,proc_date:str)->DataFrame:
    """
    Read the product_{yyyy_mm_dd}.csv from gcs
    """

    path = f"{bucket_uri}/input/products_{proc_date}.csv"

    return (
        spark.read.option("header","true")
        .option("inferSchema","true")
        .csv(path)
    )

def sculpt_scd2(df: DataFrame,proc_date:str)->DataFrame:
    """
    Add Scd -2 Book keeping columns 
    """
    #interpret proc_date as a Date
    date_lit = to_date(lit(proc_date),'yyyy_MM_dd')
    return (
        df.withColumn("effective_start_date",date_lit)
        .withColumn("effective_end_date",lit("3000-01-01").cast("date"))
    )


