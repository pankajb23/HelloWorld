from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import BaseOperator

def Sensor_0(config):
    sensor = S3KeySensor(
        bucket_key = "s3://abinitio-spark-redshift-testing/gates/2021-05-03",
        wildcard_match = False,
        aws_conn_id = "aws_default",
        verify = False,
        soft_fail = False,
        poke_interval = 60,
        timeout = 300,
        mode = "poke",
        task_id = "Sensor_0",
        exponential_backoff = False
    )

    return sensor
