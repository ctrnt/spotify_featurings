import os
import sys

from pyspark.sql import SparkSession
import networkx as nx
import matplotlib.pyplot as plt

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

    # Connexion à la base de données
    db = MySQL_DB()
    db.connection_to_mysql()
    edges_df, nodes_df = db.get_featurings_data(spark)

    # Filtrage des nœuds ayant plus de 10 featurings
    filtered_nodes_df = nodes_df.filter("nb_feats > 10")

    # Création des graphes
    G_all = nx.Graph()
    G_filtered = nx.Graph()

    # Récupérer les nœuds et définir la taille en fonction du nombre de featurings
    nodes = filtered_nodes_df.select("artist_name", "nb_feats") \
                            .rdd.map(lambda row: (row["artist_name"], {"size": row["nb_feats"]})) \
                            .collect()

    # Ajouter les nœuds au graphe
    G_all.add_nodes_from(nodes)
    G_filtered.add_nodes_from(nodes)

    # Créer une liste d'artistes filtrés
    artist_list = [row[0] for row in nodes]  # Accès par indice

    # Filtrer les arêtes pour ne garder que celles qui relient des artistes filtrés
    filtered_edges_df = edges_df.filter(
        (edges_df["artist"].isin(artist_list)) & 
        (edges_df["featuring_artist"].isin(artist_list))
    )

    # Transformer les arêtes en format exploitable par NetworkX
    filtered_edges = filtered_edges_df.select("artist", "featuring_artist", "count") \
                                    .rdd.map(lambda row: (row["artist"], row["featuring_artist"], {"weight": row["count"]})) \
                                    .collect()

    # Ajouter les arêtes au graphe
    G_all.add_edges_from(filtered_edges)

    # Positionnement des nœuds avec un layout spring, en ajustant la force de répulsion
    # pos = nx.spring_layout(G_all, k=0.05, iterations=1000, seed=42)  # Plus petit k pour rapprocher les nœuds
    pos = nx.spring_layout(G_all, k=0.2, iterations=1000, seed=42)

    # Visualisation du graphe avec des tailles de nœuds basées sur le nombre de featurings
    node_sizes = [G_all.nodes[node]["size"] * 10 for node in G_all.nodes()]  # Multiplication pour augmenter la taille
    plt.figure(figsize=(20, 20))

    # Visualisation du graphe avec des tailles de nœuds dynamiques et arêtes pondérées
    nx.draw(G_all, pos, with_labels=True, node_size=node_sizes, edge_color="gray", width=1)

    # Sauvegarde de l'image
    plt.savefig("featurings_all.png")

if __name__ == "__main__":
    visualize_featurings()
