# Databricks notebook source
# MAGIC %run "./Milestone 7, task 3"

# COMMAND ----------

#Find the most popular Pinterest category people post to based on their country.
from pyspark.sql.functions import count
from pyspark.sql.window import Window

combined_df = df_pin.join(df_geo, df_pin.ind == df_geo.ind, 'inner')
window_spec = Window.partitionBy('country').orderBy('category_count')
popular_category_df = combined_df.groupBy('country','category').agg(count('category').alias('category_count')).orderBy('country')

# COMMAND ----------

#Find how many posts each category had between 2018 and 2022.
from pyspark.sql.functions import year

posts_per_category_df = combined_df.withColumn('year', year(combined_df['timestamp']))
posts_per_category_df = posts_per_category_df.filter((posts_per_category_df['year'] >= 2018) & (posts_per_category_df['year'] <= 2022))
posts_per_category_df = posts_per_category_df.groupBy('category', 'year').agg(count('category').alias('category_count')).orderBy('year')

# COMMAND ----------

from pyspark.sql.functions import max
from pyspark.sql import functions as F

# resolves issue with joining tables on "ind"
combined_df = combined_df.select(
    df_pin.ind, 
    df_pin.unique_id, 
    df_pin.title, 
    df_pin.description,
    df_pin.follower_count, 
    df_pin.poster_name,
    df_pin.tag_list,
    df_pin.is_image_or_video,
    df_pin.image_src,
    df_pin.save_location,
    df_pin.category,
    df_geo.ind.alias('ind_geo'),
    df_geo.country,
    df_geo.coordinates,
    df_geo.timestamp
 )

#For each country find the user with the most followers.
user_combined_df = combined_df.join(df_user, combined_df.ind == df_user.ind, 'inner')
window_spec = Window.partitionBy('country').orderBy(F.desc('follower_count'))
user_with_most_followers_df = user_combined_df.withColumn(
    'max_follower_count',
    F.max('follower_count').over(window_spec)
).filter(F.col('follower_count') == F.col('max_follower_count')).select(
    'country', 'poster_name', 'follower_count'
).distinct()

#Based on the above query, find the country with the user with most followers.
country_with_most_followers = user_with_most_followers_df.groupBy('country').agg(F.max('follower_count').alias('max_follower_count')).orderBy(F.desc('max_follower_count'))


# COMMAND ----------

# What is the most popular category people post to based on the following age groups:

# 18-24
# 25-35
# 36-50
# +50

user_combined_df.createOrReplaceTempView("user_combined_df")

age_group_df = spark.sql(
    """SELECT CASE 
        WHEN age BETWEEN 18 AND 24 THEN '18-24' 
        WHEN age BETWEEN 25 AND 35 THEN '25-35' 
        WHEN age BETWEEN 36 AND 50 THEN '36-50' 
        ELSE '50+' END AS age_group, * FROM user_combined_df""")

pop_category_by_age_group_df = age_group_df.groupBy('age_group', 'category').agg(count("category").alias("category_count"))

window_spec2 = Window.partitionBy("age_group").orderBy("category_count").orderBy(F.desc("category_count"))

pop_category_by_age_group_df = pop_category_by_age_group_df.withColumn("rank", F.row_number().over(window_spec2)).filter(F.col("rank") == 1).select("age_group", "category", "category_count")

# COMMAND ----------

# What is the median follower count for users in the following age groups:

# 18-24
# 25-35
# 36-50
# +50

window_spec3 = Window.partitionBy('age_group').orderBy(F.desc('follower_count'))

median_follower_count_df = age_group_df.select('age_group', 'follower_count').groupBy('age_group').agg(F.percentile_approx("follower_count", 0.5).alias("median_follower_count")).orderBy("age_group")

# COMMAND ----------

# Find how many users have joined between 2015 and 2020.

users_joined_per_year_df = user_combined_df.withColumn('post_year', year(user_combined_df['date_joined']))
users_joined_per_year_df = users_joined_per_year_df.filter((users_joined_per_year_df['post_year'] >= 2015) & (users_joined_per_year_df['post_year'] <= 2020)).groupBy('post_year').agg(count('user_name').alias('users_joined'))


# COMMAND ----------

# Find the median follower count of users have joined between 2015 and 2020.

median_users_joined_per_year_df = user_combined_df.withColumn('post_year', year(user_combined_df['date_joined']))
median_users_joined_per_year_df = median_users_joined_per_year_df.filter((median_users_joined_per_year_df['post_year'] >= 2015) & (median_users_joined_per_year_df['post_year'] <= 2020)).groupBy('post_year').agg(F.percentile_approx("follower_count", 0.5).alias("median_follower_count"))

# COMMAND ----------

# Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
median_age_group_follower_count_df = age_group_df.withColumn('post_year', year(user_combined_df['date_joined']))
median_age_group_follower_count_df = median_age_group_follower_count_df.filter((median_age_group_follower_count_df['post_year'] >= 2015) & (median_age_group_follower_count_df['post_year'] <= 2020)).groupBy('age_group', 'post_year').agg(F.percentile_approx("follower_count", 0.5).alias("median_follower_count")).orderBy('age_group', 'post_year')

