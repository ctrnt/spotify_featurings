import os
from dotenv import load_dotenv

load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = os.getenv('SPOTIFY_CLIENT_SECRET')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')

JDBC_DRIVER_PATH = os.getenv('JDBC_DRIVER_PATH')
ARTISTS = os.getenv('ARTISTS').split(',')

DATABASE_NAME = os.getenv('DATABASE_NAME')

TOPIC_NAME = os.getenv('TOPIC_NAME')