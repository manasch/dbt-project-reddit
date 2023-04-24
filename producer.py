import json
import time
import threading
import requests

from kafka import KafkaProducer

pushshift_api_endpoint = "https://api.pushshift.io/reddit/search/comment/"
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf8'))
user_agent = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'}

def subreddit_request(api_endpoint):
    result = requests.get(api_endpoint, headers=user_agent).json()
    data = []
    if 'data' in result:
        for comment in result['data']:
            data.append({
                'subreddit': comment['subreddit'],
                'id': comment['id'],
                'body': comment['body'],
                'created_utc': comment['created_utc'],
                'utc_datetime_str': comment['utc_datetime_str'],
            })
    return data

def publish_to_topic(topic_name, payload):
    producer.send(topic_name, payload)
    producer.flush()

def main():
    subreddit_list = ["askreddit", "cricket", "conspiracy", "funnysigns", "soccer"]
    subreddits = {}
    for i in subreddit_list:
        subreddits[i+"_endpoint"] = pushshift_api_endpoint + f"?subreddit={i}&size=10"
    # subreddits = {
    #     "askreddit_endpoint" : pushshift_api_endpoint + "?subreddit=askreddit&size=10",
    #     "cricket_endpoint" : pushshift_api_endpoint + "?subreddit=cricket&size=10",
    #     "conspiracy_endpoint" : pushshift_api_endpoint + "?subreddit=conspiracy&size=10",
    #     "funnysigns_endpoint" : pushshift_api_endpoint + "?subreddit=funnysigns&size=10",
    #     "soccer_endpoint" : pushshift_api_endpoint + "?subreddit=soccer&size=10"
    # }
    # print(subreddits)
    while True:
        comments = subreddit_request(subreddits['askreddit_endpoint'])
        for comment in comments:
            publish_to_topic('askreddit', comment)
        print("sent")
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Program terminated.")
