from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Set up the Spark context and streaming context
sc = SparkContext(appName="comment_count")
ssc = StreamingContext(sc, 10)

# Set up the Kafka stream
kafka_stream = KafkaUtils.createStream(ssc, "localhost:9092",  {"ask_reddit": 1})

# Parse the JSON data and count the comments
comments_count = kafka_stream.map(lambda x: json.loads(x[1])).map(lambda comment: (comment["author"], 1)).reduceByKey(lambda a, b: a+b)

# Print the result
comments_count.pprint()

# Start the streaming context and wait for it to terminate
ssc.start()
ssc.awaitTermination()
