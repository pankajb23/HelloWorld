import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    op_branch_0 = Branch_0(config)
    op_script_0 = Script_0(config)
    op_script_1 = Script_1(config)
    op_script_2 = Script_2(config)

    op_branch_0 >> op_script_1
    op_branch_0 >> op_script_0
    op_script_1 >> op_script_2


