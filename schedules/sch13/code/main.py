import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
        print("Place your dag code here")


