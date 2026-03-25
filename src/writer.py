#writer .py
from pyspark.sql import SparkSession
from google.cloud import storage
from google.cloud import bigquery


def write_to_staging(df,project,dataset,staging_table):
    """
    Overide the staging table in bigquery
    """

    df.select("product_id","name","category","price","supplier","status","effective_start_date","effective_end_date","is_current"
    ).write.format("bigquery").option("table",f"{project}.{dataset}.{staging_table}").option("tempraryGcsBucket","dataproc-tmp-gds").mode("overwrite").save()

def merge_scd2_bq(spark:SparkSession,project:str,dataset:str,staging_table:str,target_table:str):
    """
    Expires olds scd2 rows and upinsert new/changed ones directly in biguery
    """
    key = "product_id"
    cols = ["name","category","price","supplier","status"]
    on = f"T.{key} = S.{key} and T.is_cuurent"
    changes = " OR ".join(f"T.{c} <> S.{c}" for c in cols)
    
    merge_sql = f"""
        MERGE `{project}.{dataset}.{target_table}` as T
        USING `{project}.{dataset}.{staging_table} as S
        ON {on}
        WHEN MATCHED AND {changes} then
        UPDATE set
        T.is_current = FALSE
        T.effective_end_date = S.effective_start_date 
    """

    #kick ogg yhr job onb bigquery
    client = bigquery.Client(project=project)
    job = client.query(merge_sql)
    job.result()

    insert_query = f"""
        insert into `{project}.{dataset}.{target_table}` select * from `{project}.{dataset}.{staging_table}`  
    """
    insert_staging_job = client.query(insert_query)
    insert_staging_job.result()
    print(f"Merge Completed : {job.job_id}")


def archieve_processed_csv(bucket_name: str,proc_date:str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    src = bucket.blob(f"products/input/products_{proc_date}.csv")
    dst_name  = f"products/archieve/products_{proc_date}.csv"
    bucket.copy_blob(src,bucket,dst_name)
    src.delete()

