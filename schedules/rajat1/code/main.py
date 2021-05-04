import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Branch_0 = Branch_0(config)
    _Script_0 = Script_0(config)
    _Script_1 = Script_1(config)
    _Script_2 = Script_2(config)

    _Branch_0 >> _Script_1
    _Branch_0 >> _Script_0
    _Script_1 >> _Script_2


