# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
aws_keys_df = spark.read.format("delta").load(delta_table_path)
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
NCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df_geo = spark.readStream.format('kinesis').option('streamName','streaming-0e03d1c30c91-geo').option('initialPosition','earliest').option('region','us-east-1').option('awsAccessKey', ACCESS_KEY).option('awsSecretKey', SECRET_KEY).load()

df_pin = spark.readStream.format('kinesis').option('streamName','streaming-0e03d1c30c91-pin').option('initialPosition','earliest').option('region','us-east-1').option('awsAccessKey', ACCESS_KEY).option('awsSecretKey', SECRET_KEY).load()

df_user = spark.readStream.format('kinesis').option('streamName','streaming-0e03d1c30c91-user').option('initialPosition','earliest').option('region','us-east-1').option('awsAccessKey', ACCESS_KEY).option('awsSecretKey', SECRET_KEY).load()

df_geo = df_geo.selectExpr("CAST(data AS STRING)")
df_pin = df_pin.selectExpr("CAST(data AS STRING)")
df_user = df_user.selectExpr("CAST(data AS STRING)")


# COMMAND ----------

geo_schema = StructType([
    StructField("ind", IntegerType(), False), 
    StructField("timestamp", TimestampType(), True), 
    StructField("latitude", DoubleType(), True), 
    StructField("longitude", DoubleType(), True), 
    StructField("country", StringType(), True)
    ]) 

df_geo = df_geo.withColumn("parsed_data", from_json(col("data"), geo_schema))
df_geo = df_geo.select("parsed_data.*")

df_geo = df_geo.replace({'User Info Error': None})
df_geo = df_geo.withColumn("coordinates", concat("latitude", lit(" , "), "longitude"))
df_geo = df_geo.drop("latitude", "longitude")
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

pin_schema = StructType([
    StructField("index", IntegerType(), False), 
    StructField("unique_id", StringType(), False), 
    StructField("title", StringType(), True), 
    StructField("description", StringType(), True), 
    StructField("poster_name", StringType(), True), 
    StructField("follower_count", StringType(), True), 
    StructField("tag_list", StringType(), True), 
    StructField("is_image_or_video", StringType(), True), 
    StructField("image_src", StringType(), True), 
    StructField("downloaded", IntegerType(), True), 
    StructField("save_location", StringType(), True), 
    StructField("category", StringType(), True)
    ])

df_pin = df_pin.withColumn("parsed_data", from_json(col("data"), pin_schema))
df_pin = df_pin.select("parsed_data.*")

df_pin = df_pin.replace({'User Info Error': None})
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
df_pin = df_pin.withColumnRenamed("index", "ind")
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category", "downloaded")

# COMMAND ----------

user_schema = StructType([
    StructField("ind", IntegerType(), False), 
    StructField("first_name", StringType(), True), 
    StructField("last_name", StringType(), True), 
    StructField("age", IntegerType(), True), 
    StructField("date_joined", TimestampType(), True)
    ])

df_user = df_user.withColumn("parsed_data", from_json(col("data"), user_schema))
df_user = df_user.select("parsed_data.*")

df_user = df_user.replace({'User Info Error': None})
df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
df_user = df_user.drop("first_name", "last_name")
df_user = df_user.select("ind", "user_name", "age","date_joined")

# COMMAND ----------

df_geo.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0e03d1c30c91_geo_table")

# COMMAND ----------

df_pin.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0e03d1c30c91_pin_table")

# COMMAND ----------

df_user.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/kinesis/_checkpoints/").table("0e03d1c30c91_user_table")
