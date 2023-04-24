from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, udf, col, unix_timestamp, count, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

input_topic = "askreddit"

input_schema = StructType([
    StructField('subreddit', StringType()),
    StructField('id', StringType()),
    StructField('body', StringType()),
    StructField('created_utc', TimestampType()),
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

# kafka_source.printSchema()

parsed_stream = kafka_source.selectExpr("CAST(value AS STRING)").toDF("value") \
    .select(from_json('value', input_schema).alias('data')) \
    .select('data.*')

# parsed_stream.printSchema()
windows = parsed_stream \
        .withWatermark("created_utc", "2 minutes") \
        .groupBy(window("created_utc", "10 seconds"), "subreddit")

aggregatedDF = windows.agg(count("*"))

query = aggregatedDF.writeStream \
            .outputMode("complete") \
            .option("truncate", False) \
            .format("console") \
            .start()

query.awaitTermination()


    # .withColumn('timestamp', unix_timestamp(col('utc_datetime_str'), "MM/dd/yyyy hh:mm:ss").cast(TimestampType())) \
    # .withWatermark("timestamp", "5 seconds")\
# count_aggregate = parsed_stream \
#     .withWatermark("") \
#     .groupBy(window(col("created_utc"), "5 seconds"), col("id")) \
#     .agg(count("id")) \
#     .select("subreddit", "aggrageted_count")

# # parsed_test = kafka_source.selectExpr("CAST(value AS STRING)")

# query = count_aggregate.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()

# query.awaitTermination()

# print(parsed_stream["subreddit"], parsed_stream["id"], parsed_stream["body"], parsed_stream["created_utc"], parsed_stream["utc_datetime_str"])