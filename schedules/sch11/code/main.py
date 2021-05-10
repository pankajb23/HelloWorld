import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    op_sensor_0 = Sensor_0(config)
    op_branch_0 = Branch_0(config)
    op_workflow_0 = Workflow_0(config)
    op_script_0 = Script_0(config)

    op_sensor_0 >> op_branch_0
    op_branch_0 >> op_workflow_0[0]
    op_branch_0 >> op_script_0


