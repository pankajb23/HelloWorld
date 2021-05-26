from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.models import BaseOperator

def Sensor_0(config):
    sensor = S3KeySensor(
        bucket_key = "",
        wildcard_match = False,
        aws_conn_id = "eng@prophecy.io_emr_aws_connId",
        verify = False,
        soft_fail = False,
        poke_interval = 60,
        timeout = 604800,
        mode = "poke",
        task_id = "Sensor_0",
        exponential_backoff = False
    )

    return sensor
