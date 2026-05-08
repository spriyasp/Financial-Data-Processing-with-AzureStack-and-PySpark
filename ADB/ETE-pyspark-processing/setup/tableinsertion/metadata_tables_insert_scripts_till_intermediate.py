# Databricks notebook source
dbutils.widgets.text("logic_app_url", "")
logic_app_url = dbutils.widgets.get("logic_app_url")

dbutils.widgets.text("email_id", "")
email_id = dbutils.widgets.get("email_id")

dbutils.widgets.text("storage_account", "")
storage_account = dbutils.widgets.get("storage_account")

dbutils.widgets.text("adls_url", "")
adls_url = dbutils.widgets.get("adls_url")

dbutils.widgets.text("gcp_bucket","")
gcp_bucket = dbutils.widgets.get("gcp_bucket")

dbutils.widgets.text("gcp_bucket","")
gcp_bucket = dbutils.widgets.get("gcp_bucket")

# COMMAND ----------

spark.sql(f"""
          
insert into metadata_schema.tbl_source_control values 
('{gcp_bucket}','{storage_account}','{adls_url}','landing','{logic_app_url}','{email_id}')        

""")

# COMMAND ----------

spark.sql("""

insert into metadata_schema.tbl_parameters values 
(201,'telecom/dim_cities','2001-01-01T15:13:23.963Z','dev_raw','dim_cities','dev_intermediate','dim_cities',NULL,NULL,NULL,NULL),
(202,'telecom/dim_date','2001-01-01T15:13:23.963Z','dev_raw','dim_date','dev_intermediate','dim_date',NULL,NULL,NULL,NULL),
(203,'telecom/dim_plan','2001-01-01T15:13:23.963Z','dev_raw','dim_plan','dev_intermediate','dim_plan',NULL,NULL,NULL,NULL),
(204,'telecom/fact_company','2001-01-01T15:13:23.963Z','dev_raw','fact_metrics_share','dev_intermediate','fact_metrics_share','merge into {intermediate_schema}.{intermediate_table} target
  using temp_view source
  on 
    source.seq_no = target.seq_no
  When matched then
    Update set 
      target.dated = source.dated,
      target.city_code = source.city_code,
      target.company = source.company,
      target.company_revenue_crores = source.company_revenue_crores,
      target.arpu = source.arpu,
      target.active_users_lakhs = source.active_users_lakhs,
      target.unsubscribed_users_lakhs = source.unsubscribed_users_lakhs,
      target.seq_no = source.seq_no,
      target.last_inserted_dttm_azure = source.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp = source.last_updated_dttm_gcp,
      target.load_id = source.load_id
  when not matched then
    insert (
      target.dated,
      target.city_code,
      target.company,
      target.company_revenue_crores,
      target.arpu,
      target.active_users_lakhs,
      target.unsubscribed_users_lakhs,
      target.seq_no,
      target.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp,
      target.load_id
    )
    values (
      source.dated,
      source.city_code,
      source.company,
      source.company_revenue_crores,
      source.arpu,
      source.active_users_lakhs,
      source.unsubscribed_users_lakhs,
      source.seq_no,
      source.last_inserted_dttm_azure,
      source.last_updated_dttm_gcp,
      source.load_id
    )',NULL,NULL,NULL),
(205,'telecom/fact_market','2001-01-01T15:13:23.963Z','dev_raw','fact_market_share','dev_intermediate','fact_market_share','merge into {intermediate_schema}.{intermediate_table} target
  using temp_view source
  on 
    source.seq_no = target.seq_no
  When matched then
    Update set 
      target.dated = source.dated,
      target.city_code = source.city_code,
      target.tmv_city_crores = source.tmv_city_crores,
      target.company = source.company,
      target.ms_pct = source.ms_pct,
      target.seq_no = source.seq_no,
      target.last_inserted_dttm_azure = source.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp = source.last_updated_dttm_gcp,
      target.load_id = source.load_id
  when not matched then
    insert (
      target.dated,
      target.city_code,
      target.tmv_city_crores,
      target.company,
      target.ms_pct,
      target.seq_no,
      target.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp,
      target.load_id
    )
    values (
      source.dated,
      source.city_code,
      source.tmv_city_crores,
      source.company,
      source.ms_pct,
      source.seq_no,
      source.last_inserted_dttm_azure,
      source.last_updated_dttm_gcp,
      source.load_id
    )',NULL,NULL,NULL),
(206,'telecom/fact_plan','2001-01-01T15:13:23.963Z','dev_raw','fact_plan_revenue','dev_intermediate','fact_plan_revenue','merge into {intermediate_schema}.{intermediate_table} target
  using temp_view source
  on 
    source.seq_no = target.seq_no
  When matched then
    Update set 
      target.dated = source.dated,
      target.city_code = source.city_code,
      target.plan = source.plan,
      target.plan_revenue_crores = source.plan_revenue_crores,
      target.seq_no = source.seq_no,
      target.last_inserted_dttm_azure = source.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp = source.last_updated_dttm_gcp,
      target.load_id = source.load_id
  when not matched then
    insert (
      target.dated,
      target.city_code,
      target.plan,
      target.plan_revenue_crores,
      target.seq_no,
      target.last_inserted_dttm_azure,
      target.last_updated_dttm_gcp,
      target.load_id
    )
    values (
      source.dated,
      source.city_code,
      source.plan,
      source.plan_revenue_crores,
      source.seq_no,
      source.last_inserted_dttm_azure,
      source.last_updated_dttm_gcp,
      source.load_id
    )',NULL,NULL,NULL)
""")
