# Databricks notebook source
# MAGIC %run "./Milestone 7, task 2"

# COMMAND ----------

from pyspark.sql.functions import concat, lit, to_timestamp

df_user = df_user.replace({'User Info Error': None})
df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))
df_user = df_user.drop("first_name", "last_name")
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))
df_user = df_user.select("ind", "user_name", "age","date_joined")
