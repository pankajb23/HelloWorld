import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    op_script_0 = Script_0(config)

    


