import json
import time
import threading

import mysql.connector
from kafka import KafkaConsumer

mydb = mysql.connector.connect(
    host="localhost",
    database="reddit",
    user='spark',
    password='pwd'
)

table = "comments"
lock = threading.Lock()
topics = ["askreddit", "cricket"]

cur = mydb.cursor(
    buffered=True,
    dictionary=True
)

# cur.execute(f"select * from {table}")
# print(cur.fetchall())

def insert_into_database(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf8')))

    for record in consumer:
        comment = record.value

        try:
            # query = f"insert into {table} values (id, subreddit, body, created_utc) values (%s, %s, %s, %s)"
            lock.acquire()
            cur.execute(f"insert into {table} values (\"{(comment['id'])}\", \"{(comment['subreddit'])}\", \"{(comment['body']).encode().hex()}\", {comment['created_utc']})")
            mydb.commit()
            lock.release()
            print("Inserted into", topic)
        except Exception as e:
            print(e)

def main():
    threads = []
    for topic in topics:
        t = threading.Thread(target=insert_into_database, args=(topic,))
        print("Database thread started for", topic)
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
        print("Thread stopped")

if __name__ == "__main__":
    main()
