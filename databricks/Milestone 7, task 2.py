# Databricks notebook source
# MAGIC %run "./Milestone 7, task 1"

# COMMAND ----------

from pyspark.sql.functions import concat, lit, to_timestamp

df_geo = df_geo.replace({'User Info Error': None})
df_geo = df_geo.withColumn("coordinates", concat("latitude", lit(" , "), "longitude"))
df_geo = df_geo.drop("latitude", "longitude")
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")
