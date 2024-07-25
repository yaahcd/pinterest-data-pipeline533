# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------


file_location = "/mnt/pinterest_data/topics/0e03d1c30c91.pin/partition=0/*.json" 
file_type = "json"
infer_schema = "true"

df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

display(df_pin)

# COMMAND ----------

file_location = "/mnt/pinterest_data/topics/0e03d1c30c91.geo/partition=0/*.json" 
file_type = "json"
infer_schema = "true"

df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

display(df_geo)

# COMMAND ----------

file_location = "/mnt/pinterest_data/topics/0e03d1c30c91.user/partition=0/*.json" 
file_type = "json"
infer_schema = "true"

df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)

display(df_user)
