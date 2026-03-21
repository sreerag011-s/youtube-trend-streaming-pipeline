import os
import json
import time
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
NICHE = os.getenv("YOUTUBE_NICHE")
TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_SERVER = os.getenv("KAFKA_SERVER")
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")

print("Kafka server:", KAFKA_SERVER)
print("Topic:", TOPIC)

youtube = build("youtube", "v3", developerKey=API_KEY)

# Correct configuration for Azure Event Hub Kafka
conf = {
    "bootstrap.servers": KAFKA_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": EVENT_HUB_CONNECTION_STRING,
    "client.id": "youtube-producer"
}

producer = Producer(conf)

processed_videos = set()


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed:", err)
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[{msg.partition()}] offset {msg.offset()}"
        )


def send_event(event):

    try:

        producer.produce(
            TOPIC,
            key=event["video_id"],
            value=json.dumps(event),
            callback=delivery_report
        )

        producer.poll(0)

    except BufferError:
        print("Local producer queue full, flushing...")
        producer.flush()


def fetch_trending():

    print(f"\nFetching videos for niche: {NICHE}\n")

    try:

        search_request = youtube.search().list(
            part="snippet",
            q=NICHE,
            type="video",
            order="viewCount",
            maxResults=10
        )

        search_response = search_request.execute()

    except Exception as e:
        print("YouTube API error:", e)
        return

    video_ids = []

    for item in search_response.get("items", []):
        video_ids.append(item["id"]["videoId"])

    if not video_ids:
        print("No videos returned from search")
        return

    try:

        stats_request = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(video_ids)
        )

        stats_response = stats_request.execute()

    except Exception as e:
        print("YouTube stats fetch failed:", e)
        return

    for video in stats_response.get("items", []):

        video_id = video["id"]

        if video_id in processed_videos:
            continue

        processed_videos.add(video_id)

        event = {
            "video_id": video_id,
            "title": video["snippet"]["title"],
            "channel": video["snippet"]["channelTitle"],
            "published_at": video["snippet"]["publishedAt"],
            "views": int(video["statistics"].get("viewCount", 0)),
            "likes": int(video["statistics"].get("likeCount", 0)),
            "niche": NICHE,
            "ingestion_time": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        print("Sending event:", video_id)

        send_event(event)

    producer.flush()


if __name__ == "__main__":

    for i in range(3):

        fetch_trending()

        print("\nSleeping 60 seconds...\n")

        time.sleep(60)

    producer.flush()