from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, udf, col, unix_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

input_topic = "askreddit"

input_schema = StructType([
    StructField('subreddit', StringType()),
    StructField('id', StringType()),
    StructField('body', StringType()),
    StructField('created_utc', StringType()),
    StructField('utc_datetime_str', StringType())
])

spark = SparkSession \
    .builder \
    .appName("RedditStreamApp") \
    .getOrCreate()

kafka_source = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', input_topic) \
    .load()

parsed_stream = kafka_source.selectExpr("CAST(value AS STRING)") \
    .select(from_json('value', input_schema).alias('data')) \
    .select('data.*')

# parsed_test = kafka_source.selectExpr("CAST(value AS STRING)")

query = parsed_stream.writeStream \
        .format("console") \
        .start()

query.awaitTermination()

# print(parsed_stream["subreddit"], parsed_stream["id"], parsed_stream["body"], parsed_stream["created_utc"], parsed_stream["utc_datetime_str"])