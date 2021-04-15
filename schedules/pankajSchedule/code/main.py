import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Workflow_0 = Workflow_0(config)
    _Workflow_2 = Workflow_2(config)
    _Email_1 = Email_1(config)
    _Email0 = Email0(config)

    _Workflow_0[1] >> _Email0
    _Workflow_0[1] >> _Workflow_2[0]
    _Workflow_2[1] >> _Email_1


