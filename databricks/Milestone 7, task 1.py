# Databricks notebook source
# MAGIC %run "./Milestone 6, task 2"

# COMMAND ----------

df_pin = df_pin.replace({'User Info Error': None})
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))
df_pin = df_pin.withColumnRenamed("index", "ind")
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")
