# Databricks notebook source
dbutils.widgets.text("storage_account_name", "")
storage_account_name = dbutils.widgets.get("storage_account_name")
dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

# COMMAND ----------


# tbl_parameters holds the parameter values of DDL
spark.sql(f"""
create or replace table PT.{env}_metadata_schema.tbl_parameters (

    job_id int,
    job_name string,
    source_file_path string,
    watermark_column timestamp,
    load_type string,
    logic_app_url string,
    layer string
    
)
location 'abfss://metadata@{storage_account_name}.dfs.core.windows.net/tbl_parameters'
""")
