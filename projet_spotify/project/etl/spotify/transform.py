import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from project.scripts.datacleaning import DataCleaning
from project.config.logging_config import setup_logging
from pyspark.sql import SparkSession, DataFrame

def transform(spark:SparkSession, dataframe:DataFrame):
    logger = setup_logging()

    try:
        logger = setup_logging()
        logger.info("Starting data transformation process")

        datacleaner = DataCleaning(spark, dataframe)
        datacleaner.drop_duplicates()
        datacleaner.create_artists_dataframe()
        datacleaner.create_featurings_dataframe()
        datacleaner.create_artists_tracks_dataframe()
        logger.info("Data transformation process completed")

        return datacleaner
    
    except Exception as e:
        logger.info(f"Error in data transformation process: {e}")
        spark.stop()