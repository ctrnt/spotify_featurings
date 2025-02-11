import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

from project.config.logging_config import setup_logging
from project.config.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, DATABASE_NAME

class MySQL_DB():
    def __init__(self):
        self.host = MYSQL_HOST
        self.port = MYSQL_PORT
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = DATABASE_NAME
        self.connection = None
        self.logger = setup_logging()

    def connection_to_mysql(self):
        try:
            connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            self.connection = connection
            self.logger.info("Connected to MySQL Host")
        except Exception as e:
            self.logger.error(f"Error connecting to MySQL Host: {e}")

    def _check_db_exists(self):
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            for db in databases:
                if db[0] == self.database:
                    self.logger.info(f"Database already exists")
                    return True
            return False

    def _create_tables(self):
        tables = {"artists": "artist_id INT AUTO_INCREMENT PRIMARY KEY, artist_name VARCHAR(255)",
                  "featurings": "track_id INT AUTO_INCREMENT PRIMARY KEY, track_name VARCHAR(255), spotify_track_id VARCHAR(255)",
                  "artist_featurings": "artist_id INT, track_id INT, FOREIGN KEY (artist_id) REFERENCES artists(artist_id), FOREIGN KEY (track_id) REFERENCES featurings(track_id)"}
        
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE `{self.database}`")
            for table_name, columns in tables.items():
                try:
                    cursor.execute(f"CREATE TABLE `{table_name}` ({columns})")
                    self.logger.info(f"Table {table_name} created")
                except Exception as e:
                    self.logger.error(f"Error creating table {table_name}: {e}")

    def create_db(self):
        if self._check_db_exists() == False:
            with self.connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE `{self.database}`")
                self.logger.info(f"Database {self.database} created")
                self._create_tables()

    def insert_df_in_db(self, df, table_name):
        df.write \
          .format("jdbc") \
          .option("url", f"jdbc:mysql://{self.host}/{self.database}") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("dbtable", table_name) \
          .option("user", self.user) \
          .option("password", self.password) \
          .mode("append") \
          .save()
        
        self.logger.info(f"Data inserted into {table_name} table")

    def get_featurings_data(self, spark):
        jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
        connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        query_weights = """
        (SELECT a.artist_name AS artist, b.artist_name AS featuring_artist, COUNT(*) AS count
        FROM artist_featurings af
        JOIN artists a ON af.artist_id = a.artist_id
        JOIN artist_featurings af2 ON af.track_id = af2.track_id
        JOIN artists b ON af2.artist_id = b.artist_id
        WHERE a.artist_id != b.artist_id
        GROUP BY a.artist_name, b.artist_name) AS weights
        """

        weights_df = spark.read.jdbc(url=jdbc_url, table=query_weights, properties=connection_properties)
        
        query_nb_feats = """
        (SELECT artists.artist_name, COUNT(artist_featurings.artist_id) AS nb_feats
        FROM artist_featurings
        JOIN artists ON artists.artist_id = artist_featurings.artist_id
        GROUP BY artists.artist_name) AS nb_feats
        """

        nb_feats_df = spark.read.jdbc(url=jdbc_url, table=query_nb_feats, properties=connection_properties)
        return weights_df, nb_feats_df