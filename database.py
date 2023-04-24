import json
import time

import mysql.connector
from kafka import KafkaConsumer

mydb = mysql.connector.connect(
    host="localhost",
    database="reddit",
    user='spark',
    password='pwd'
)

table = "comments"

cur = mydb.cursor(
    buffered=True,
    dictionary=True
)

cur.execute(f"select * from {table}")
print(cur.fetchall())

consumer = KafkaConsumer("askreddit", bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf8')))

for record in consumer:
    comment = record.value

    try:
        query = f"insert into {table} values (id, subreddit, body, created_utc) values (%s, %s, %s, %s)"
        cur.execute(f"insert into {table} values (\"{(comment['id'])}\",\"{(comment['subreddit'])}\", \"{(comment['body']).encode().hex()}\", {comment['created_utc']})")
        mydb.commit()
        print("Inserted")
    except Exception as e:
        print(e)
