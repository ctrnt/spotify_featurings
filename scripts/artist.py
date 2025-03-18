import json
import time

from requests import get

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from project.config.logging_config import setup_logging

class Artist():

    SEARCH_URL = "https://api.spotify.com/v1/search"
    ARTIST_URL = "https://api.spotify.com/v1/artists"
    ALBUM_URL = "https://api.spotify.com/v1/albums"
    RETRY_LIMIT = 10
    WAIT_TIME = 40

    def __init__(self, headers:list[str:str], spark:SparkSession, schema:StructType) -> None:
        self.headers = headers
        self.spark = spark
        self.schema = schema
        self.df = None
        self.name = None
        self.id = None
        self.logger = setup_logging()

    def get_response(self, url, params):
        try:
            response = get(url, headers=self.headers, params=params)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 1))
                self.logger.warning(f"{self.name} | Limit exceeded : waiting for 2*{retry_after}s")
                time.sleep(retry_after * 2)

                # if retry_after >= self.RETRY_LIMIT:
                #     self.logger.warning(f"{self.name} | Limit exceeded : waiting for 40s")
                #     time.sleep(self.WAIT_TIME)
                # else:
                #     self.logger.warning(f"{self.name} | Limit exceeded : waiting for {retry_after*2}s")
                #     time.sleep(retry_after * 2)

                return self.get_response(url, params)
            
            else:
                response = json.loads(response.content)
                return response
                
        except Exception as e:
            self.logger.error(f"An error occured while getting response : {e}")

    def get_artist_id(self, artist_name):
        params = {
            "q":artist_name,
            "type":"artist",
            "market":"FR",
            "limit":1,
            "offset":0
        }

        response = self.get_response(self.SEARCH_URL, params)
        response = response["artists"]["items"][0]
        self.id = response["id"]
        self.name = response["name"]

    def get_feats_from_album_tracks(self, item):
        filtered_data = []
        artists = item["artists"]
        if len(artists) > 1:
            filtered_artists = []
            for artist in artists:  
                filtered_artists.append(artist["name"])
            if self.name in filtered_artists:
                filtered_data = {
                    "track_name": item["name"],
                    "spotify_track_id": item["id"],
                    "main_artist": filtered_artists[0],
                    "feature_artists": filtered_artists[1:]
                }
        return filtered_data
    
    def get_album_tracks(self, album_id):
        url = f"{self.ALBUM_URL}/{album_id}/tracks"
        params = {
            "include_groups":"appears_on",
            "market": "FR",
            "limit": 50,
            "offset": 0
        }

        response = self.get_response(url, params)
        response = response["items"]

        list_feats = []
        for track in response:
            track = self.get_feats_from_album_tracks(track)
            if type(track)!=list:
                list_feats.append(track)

        return list_feats

    def get_artist_features(self):
        features_data = []
        count =0
        url = f"{self.ARTIST_URL}/{self.id}/albums"
        params = {
            "include_groups":"album,appears_on",
            "market": "FR",
            "limit": 50,
            "offset": count
        }

        response = self.get_response(url, params)
        nb_feats = response["total"]

        if nb_feats > 50:
            while count < nb_feats:
                response = self.get_response(url, params)
                response = response["items"]

                for item in response:
                    if item["album_type"]!="compilation":
                        id = item["id"]
                        item_feats = self.get_album_tracks(id)
                        features_data.extend(item_feats)
            
                count+=50
                params["offset"] = count

        else:
            response = response["items"]
            for item in response:
                    if item["album_type"]!="compilation":
                        id = item["id"]
                        item_feats = self.get_album_tracks(id)
                        features_data.extend(item_feats)

        self.logger.info(f"{self.name} has been processed")
        self.df = self.spark.createDataFrame(features_data, self.schema)