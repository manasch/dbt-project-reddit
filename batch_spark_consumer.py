import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, udf, col, unix_timestamp, count, window, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
        .appName('BatchReddit') \
        .config('spark.jars', 'bin/mysql-connector-java-8.0.30.jar') \
        .getOrCreate()

    # .option('dbtable', 'comments') \
df = spark.read \
    .format('jdbc') \
    .option('driver', 'com.mysql.cj.jdbc.Driver') \
    .option('url', 'jdbc:mysql://localhost:3306/reddit') \
    .option('query', "select subreddit, count(*) from comments group by subreddit") \
    .option('user', 'spark') \
    .option('password', 'pwd') \
    .load()

df.show()
