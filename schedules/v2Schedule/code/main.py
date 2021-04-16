import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Workflow_0 = Workflow_0(config)
    _Email_1 = Email_1(config)
    _Branch_0 = Branch_0(config)
    _Script_0 = Script_0(config)
    _Workflow_1 = Workflow_1(config)
    _Email_0 = Email_0(config)

    _Workflow_0[1] >> _Email_1
    _Email_1 >> _Branch_0
    _Branch_0 >> _Script_0
    _Script_0 >> _Email_0
    _Workflow_1[1] >> _Email_0
    _Workflow_0[1] >> _Workflow_1[0]
    _Branch_0 >> _Workflow_1[0]


