#main.py
import argparse
from datetime import datetime

import spark_builder
import etl 
import deequ_checks
import writer 
import sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date",help="yyyy-MM-dd to process",required=False)

    args = p.parse_args()

    proc_date = args.date or datetime.utcnow().strftime("%Y_%m_%d")

    spark = spark_builder.get_spark("product diamention SCD")

    #1) read
    df = etl.read_daily_data(spark,"gs://spark-datasets-kd/products",proc_date)

    # 2) Quality
    deequ_checks.run_quality_checks(spark,df)

    # 3) sculpt scd2
    df2 = etl.sculpt_scd2(df,proc_date)

    # 4) stage
    writer.write_to_staging(df2,"project-b33d6da0-bcfc-471f-99c","product_dwh","dim_products_staging")

    # 5) merge 
    writer.merge_scd2_bq(spark,"project-b33d6da0-bcfc-471f-99c","product_dwh","dim_products_staging","dim_products")

    #archieve
    writer.archieve_processed_csv("spark-datasets-kds",proc_date)

    spark.stop()

    sys.exit(0)

if __name__ == "__main__":
    main()
    
