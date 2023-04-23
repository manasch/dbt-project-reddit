import json
import threading
import requests

from kafka import KafkaProducer

pushshift_api_endpoint = "https://api.pushshift.io/reddit/search/comment/"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf8'))

def subreddit_request(api_endpoint):
    res = requests.get(api_endpoint)
    return res.json()

def main():
    askreddit_endpoint = pushshift_api_endpoint + "?subreddit=askreddit&limit=1"
    producer.send("askreddit", subreddit_request(askreddit_endpoint))

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Program terminated.")
