from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_function():
    print("Hello from Airflow DAG!")

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 17),
}

dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval=None,  # Exécution manuelle
)

# Tâche pour exécuter le script
run_script = PythonOperator(
    task_id='run_test_script',
    python_callable=test_function,
    dag=dag,
)

run_script
