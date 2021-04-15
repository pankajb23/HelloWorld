import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Email_0 = Email_0(config)
    _Workflow_0 = Workflow_0(config)

    _Email_0 >> _Workflow_0[0]


