# Databricks notebook source

spark.sql(f"""
insert into PT.dev_metadata_schema.tbl_parameters values 
(101,'gcp-landing','dim_tables/products','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'landing'),
(101,'gcp-landing','dim_tables/tier_details','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'landing'),
(102,'landing-bronze','dim_tables/products','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'bronze'),
(102,'landing-bronze','dim_tables/tier_details','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'bronze'),
(103,'bronze-silver','dim_tables/products','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'silver'),
(103,'bronze-silver','dim_tables/tier_details','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'silver'),
(104,'silver-gold','dim_tables/products','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'gold'),
(104,'silver-gold','dim_tables/tier_details','2001-01-01T15:13:23.963Z',
'full_load',
NULL,'gold')
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC update PT.dev_metadata_schema.tbl_parameters
# MAGIC set load_type='inc' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from PT.dev_metadata_schema.tbl_parameters
