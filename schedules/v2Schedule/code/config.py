from airflow.models import Variable


class Config(object):
    def __init__(self, fabric: str = "common"):
        self.fabric = fabric

        self.dag_id = "v2Schedule"
        self.timezone = "US/Pacific"
        self.start_date = "2021-01-01T00:00:00"
        self.schedule_interval = "@daily"
        self.connection_id = "eng.pankaj.002@prophecy.io_49_connId"
        

    def dag_args(self):
        return {
            'dag_id': self.dag_id,
            'default_args': {
                "depends_on_past": False,
                "start_date": self.start_date
            },
            "schedule_interval": self.schedule_interval
        }
