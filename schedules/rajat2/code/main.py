import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    op_test_script = Test_Script(config)

    


