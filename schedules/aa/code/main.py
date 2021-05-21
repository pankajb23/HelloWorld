import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as dag:
        dag.configuration = config
        print("Place your dag code here")


