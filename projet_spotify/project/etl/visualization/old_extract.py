import os
import sys
import networkx as nx
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from project.scripts.mysql_db import MySQL_DB
from project.config.config import JDBC_DRIVER_PATH

def visualize_featurings():
    spark = SparkSession.builder \
        .appName("SpotifySpark") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.cores", "2") \
        .master("local[2]") \
        .getOrCreate()

    db = MySQL_DB()
    db.connection_to_mysql()
    df = db.get_featurings_data(spark).toPandas()  # Convert Spark DataFrame to Pandas DataFrame

    # G = nx.Graph()

    # for _, row in df.iterrows():
    #     if row['count'] > 1:  # Only add nodes and edges for artists with more than 1 featuring
    #         artist_name = row['artist']
    #         featuring_artist_name = row['featuring_artist']
    #         G.add_node(artist_name)
    #         G.add_node(featuring_artist_name)
    #         G.add_edge(artist_name, featuring_artist_name, weight=row['count'])

    # # Adjust spring_layout parameters
    # pos = nx.spring_layout(G, k=0.1, iterations=100, weight='weight')
    # sizes = [G.degree(node, weight='weight') * 100 for node in G.nodes()]

    # plt.figure(figsize=(12, 12))
    # nx.draw(G, pos, with_labels=True, labels={node: node for node in G.nodes()}, node_size=sizes, node_color='skyblue', edge_color='gray', linewidths=1, font_size=10)
    # plt.title("Featurings entre artistes")
    # plt.savefig("featurings_visualization.png")  # Save the figure
    # plt.close()  # Close the figure to free up memory

    spark.stop()

if __name__ == "__main__":
    visualize_featurings()