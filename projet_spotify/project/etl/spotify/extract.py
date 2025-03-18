import os
import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from confluent_kafka import Consumer, KafkaException

from project.scripts.artist import *
from project.scripts.spotify_connector import *
from project.config.logging_config import setup_logging
from project.config.config import JDBC_DRIVER_PATH, TOPIC_NAME

def extract():
    logger = setup_logging()

    try:
        spark = SparkSession.builder \
            .appName("SpotifySpark") \
            .config("spark.jars", JDBC_DRIVER_PATH) \
            .master("local[*]") \
            .getOrCreate()
              
        logger.info("Starting data extraction process")

        api_connector = Connector()
        headers = api_connector.get_auth_headers()

        schema = StructType([
                StructField("track_name", StringType(), True),
                StructField("spotify_track_id", StringType(), True),
                StructField("main_artist", StringType(), True),
                StructField("feature_artists", ArrayType(StringType()), True)
            ])
        
        artists_df = spark.createDataFrame([], schema)

        consumer_config = {
            'bootstrap.servers': 'localhost:9092',  
            'group.id': 'spotify-consumer-group',  
            'auto.offset.reset': 'earliest'  
        }
        consumer = Consumer(consumer_config)

        def basic_consume_loop(consumer, topic):
            consumer.subscribe(topic)

            msg = consumer.poll(timeout=1.0)

            if msg is None:
                None

            if msg.error():
                raise KafkaException(msg.error())

            else:
                artist_name = msg.value().decode('utf-8')
                artist_instance = Artist(headers, spark, schema)
                artist_instance.get_artist_id(artist_name)
                artist_instance.get_artist_features()

                logger.info(f"{artist_name}:{artist_instance.df.count()}")

        basic_consume_loop(consumer, TOPIC_NAME)

        return spark, artists_df

    except Exception as e:
        logger.error(f"Error : {e}")
        spark.stop()
        raise

if __name__ == "__main__":
    spark, artists_df = extract()
    spark.stop()