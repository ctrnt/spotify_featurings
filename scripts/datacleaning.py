from pyspark.sql.functions import row_number, explode, col
from pyspark.sql.window import Window
from project.config.logging_config import setup_logging

class DataCleaning():
    def __init__(self, spark, dataframe):
        self.logger = setup_logging()
        self.spark = spark
        self.df = dataframe
        self.artists_df = None
        self.featurings_df = None
        self.artist_featurings_df = None

    def drop_duplicates(self):
        self.df = self.df.dropDuplicates(["spotify_track_id"])
        self.df = self.df.dropDuplicates(["track_name", "main_artist", "feature_artists"])

    def create_artists_dataframe(self):
        main_artists_df = self.df.select(col("main_artist").alias("artist_name"))
        feature_artists_df = self.df.select(explode(col("feature_artists")).alias("artist_name"))
        artists_df = main_artists_df.union(feature_artists_df).distinct()

        window_spec = Window.orderBy("artist_name")
        artists_df = artists_df.withColumn("artist_id", row_number().over(window_spec))

        self.artists_df = artists_df
        self.logger.info("Artists dataframe created")

        main_artists_df.unpersist()
        feature_artists_df.unpersist()

        return artists_df

    def create_featurings_dataframe(self):
        featurings_df = self.df.select("track_name", "spotify_track_id").distinct()
        window_spec = Window.orderBy("track_name")
        featurings_df = featurings_df.withColumn("track_id", row_number().over(window_spec))

        self.featurings_df = featurings_df
        self.logger.info("Featurings dataframe created")

        return featurings_df
    
    def create_artists_tracks_dataframe(self):
        main_artists_tracks_df = self.df.select("spotify_track_id", col("main_artist").alias("artist_name"))
        main_artists_tracks_df = main_artists_tracks_df.join(self.artists_df, on="artist_name")
        main_artists_tracks_df = main_artists_tracks_df.join(self.featurings_df, on="spotify_track_id").select("artist_id", "track_id")

        feature_artists_tracks_df = self.df.select("spotify_track_id", explode(col("feature_artists")).alias("artist_name"))
        feature_artists_tracks_df = feature_artists_tracks_df.join(self.artists_df, on="artist_name")
        feature_artists_tracks_df = feature_artists_tracks_df.join(self.featurings_df, on="spotify_track_id").select("artist_id", "track_id")

        artist_featurings_df = main_artists_tracks_df.union(feature_artists_tracks_df).distinct()

        valid_artist_ids = self.artists_df.select("artist_id").rdd.flatMap(lambda x: x).collect()
        artist_featurings_df = artist_featurings_df.filter(artist_featurings_df.artist_id.isin(valid_artist_ids))
        
        self.artist_featurings_df = artist_featurings_df
        self.logger.info("Artists_Featurings dataframe created")

        main_artists_tracks_df.unpersist()
        feature_artists_tracks_df.unpersist()

        return artist_featurings_df
