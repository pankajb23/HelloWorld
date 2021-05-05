import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Test_Script = Test_Script(config)

    


