import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from project.scripts.mysql_db import MySQL_DB
from project.config.logging_config import setup_logging
from project.scripts.datacleaning import DataCleaning

def load(datacleaner:DataCleaning):
    try:
        logger = setup_logging()
        logger.info("Starting data loading process")

        mysql_db = MySQL_DB()
        mysql_db.connection_to_mysql()
        mysql_db.create_db()
        mysql_db.insert_df_in_db(datacleaner.artists_df, "artists")
        mysql_db.insert_df_in_db(datacleaner.featurings_df, "featurings")
        mysql_db.insert_df_in_db(datacleaner.artist_featurings_df, "artist_featurings")

        logger.info("Data loading process finished")
        datacleaner.spark.stop()

    except Exception as e:
        logger.error(f"Error in data loading process: {e}")
        datacleaner.spark.stop()