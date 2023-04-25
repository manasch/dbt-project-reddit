#0tmijO9FlgGMd62IR8pb9yCMOzv6vg
import json
import time
import threading
import praw

from kafka import KafkaProducer

reddit = praw.Reddit(client_id='3N95R_vpvHj7IFTnM8reXw',
                     client_secret='0tmijO9FlgGMd62IR8pb9yCMOzv6vg',
                     user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36')
                     
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf8'))

recieved = []               # maintain a list of recieved comments

def subreddit_request(subreddit_name):
    data = []
    for submission in reddit.subreddit(subreddit_name).new(limit=10):
    	for comment in submission.comments:
        	if comment.id not in recieved:
                data.append({
                    'subreddit': subreddit_name,
                    'id': comment.id,
                    'body': comment.body,
                    'created_utc': int(comment.created_utc),
                })
                recieved.append(comment.id)
    return data

def publish_to_topic(topic_name, payload):
    producer.send(topic_name, payload)
    producer.flush()

def main():
    subreddit_list = ["askreddit", "cricket", "conspiracy", "funnysigns", "soccer"]
    
    while True:
        for subreddit_name in subreddit_list:
            comments = subreddit_request(subreddit_name)
            for comment in comments:
                publish_to_topic(subreddit_name, comment)
            print(f"sent {len(comments)} comments from r/{subreddit_name}")
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Program terminated.")

