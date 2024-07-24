# Databricks notebook source
from pyspark.sql.functions import *
import urllib

delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

aws_keys_df = spark.read.format("delta").load(delta_table_path)

ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

AWS_S3_BUCKET = "user-0e03d1c30c91-bucket"

MOUNT_NAME = "/mnt/pinterest_data"

SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)

dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

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
