import os
import json
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

conf = {
    "bootstrap.servers": os.getenv("KAFKA_SERVER"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": os.getenv("EVENT_HUB_CONNECTION_STRING"),
}

producer = Producer(conf)

producer.produce(
    os.getenv("KAFKA_TOPIC"),
    key="test",
    value=json.dumps({"message": "hello"})
)

producer.flush()

print("Message sent successfully")