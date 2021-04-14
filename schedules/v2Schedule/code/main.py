import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Workflow_0 = Workflow_0(config)
    _Branch_0 = Branch_0(config)
    _Email_1 = Email_1(config)
    _Email_Test123 = Email_Test123(config)

    _Branch_0 >> _Email_1
    _Workflow_0[1] >> _Email_Test123
    _Workflow_0[1] >> _Branch_0


