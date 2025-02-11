import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from project.scripts.artist import *
from project.scripts.spotify_connector import *
from project.config.logging_config import setup_logging
from project.config.config import JDBC_DRIVER_PATH, ARTISTS

def extract():
    logger = setup_logging()

    try:
        spark = SparkSession.builder \
            .appName("SpotifySpark") \
            .config("spark.jars", JDBC_DRIVER_PATH) \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.driver.cores", "2") \
            .master("local[2]") \
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

        for artist_name in ARTISTS:
            artist_instance = Artist(headers, spark, schema)
            artist_instance.get_artist_id(artist_name)
            artist_instance.get_artist_features()

            artists_df = artists_df.union(artist_instance.df)
        
        logger.info("Data extraction process completed")

        return spark, artists_df

    except Exception as e:
        logger.error(f"Error : {e}")
        spark.stop()
        raise

if __name__ == "__main__":
    spark, artists_df = extract()
    spark.stop()