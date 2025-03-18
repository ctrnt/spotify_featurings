from confluent_kafka import Producer
from project.config.config import ARTISTS
import sys
import os
import json
import time

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'spotify-producer'
}

producer = Producer(conf)

topic = "spotify-topic"
for artist in ARTISTS:
    producer.produce(topic, value=artist)
    producer.flush()
    time.sleep(1)

print("🚀 Tous les artistes ont été envoyés !")