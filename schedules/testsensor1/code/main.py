import json
from airflow import DAG

config = load_config()
with DAG(**config.dag_args()) as the_dag:
    _Sensor_0 = Sensor_0(config)
    _Email_0 = Email_0(config)

    _Sensor_0 >> _Email_0


