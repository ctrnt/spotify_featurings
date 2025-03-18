from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

# Définition des paramètres par défaut pour le DAG
default_args = {
    'owner': 'airflow',  # Indique qui est responsable du DAG
    'start_date': datetime(2025, 3, 12),  # Date de début du DAG
    'retries': 1,  # Nombre de tentatives en cas d'échec
    'retry_delay': timedelta(minutes=5),  # Temps d'attente entre les tentatives
}

# Création du DAG
dag = DAG(
    'weekly_kafka_processing',  # Nom du DAG
    default_args=default_args,  # Paramètres par défaut définis plus haut
    schedule_interval='0 2 * * 1',  # Planification : Tous les lundis à 2h du matin
    catchup=False,  # Empêche d'exécuter les DAGs manqués si Airflow a été arrêté
)

# Tâche 1 : Lancer le Producer
run_producer = DockerOperator(
    task_id='run_producer',  # Nom unique de la tâche dans Airflow
    image='my_producer_image',  # Image Docker du producer
    container_name='kafka_producer',  # Nom du conteneur Docker
    auto_remove=True,  # Supprime le conteneur une fois terminé
    command='python3 /app/producer.py',  # Commande exécutée dans le conteneur
    dag=dag,  # Associe la tâche à notre DAG
)

# Tâches 2 : Lancer plusieurs Consumers en parallèle
for i in range(4):  # Boucle pour créer 4 Consumers
    consumer_task = DockerOperator(
        task_id=f'run_consumer_{i}',  # Nom unique pour chaque consumer
        image='my_consumer_image',  # Image Docker du consumer
        container_name=f'kafka_consumer_{i}',  # Nom unique du conteneur
        auto_remove=True,  # Supprime le conteneur après exécution
        command='python3 /app/extract.py',  # Exécute le script consumer
        dag=dag,  # Associe la tâche au DAG
    )

    consumer_task.set_upstream(run_producer)  # Exécute le consumer après le producer
