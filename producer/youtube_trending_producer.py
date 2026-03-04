import os
import json
import time
from kafka import KafkaProducer
from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
TOPIC = os.getenv("KAFKA_TOPIC")

youtube = build("youtube", "v3", developerKey=API_KEY)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

MAX_BATCHES = 3
SLEEP_TIME = 60

def fetch_trending():

    request = youtube.videos().list(
        part="snippet,statistics",
        chart="mostPopular",
        regionCode="IN",
        maxResults=25
    )

    response = request.execute()

    for item in response["items"]:

        video_data = {

            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "published_at": item["snippet"]["publishedAt"],
            "views": item["statistics"].get("viewCount", 0),
            "likes": item["statistics"].get("likeCount", 0)

        }

        producer.send(TOPIC, video_data)

        print("Produced:", video_data)


for batch in range(MAX_BATCHES):

    print(f"\nBatch {batch+1} running...\n")

    fetch_trending()

    time.sleep(SLEEP_TIME)

print("\nProducer finished.")