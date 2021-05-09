


class Config(object):

    def __init__(self, fabric: str="common"):
        self.fabric = fabric
        self.dag_id = "Ruchir_Test_URL"
        self.timezone = "US/Pacific"
        self.start_date = "2021-05-05T06:28:29"
        self.schedule_interval = "@daily"
        self.Test = "test"
        self.cluster_id = "eng.pankaj.002@prophecy.io_49_connId"

        if fabric == "azdb":
            self.start_date = "2021-05-05T06:28:29"

    def dag_args(self):
        return {
            'dag_id': self.dag_id,
            'default_args': {
"depends_on_past" : False, "start_date" : self.start_date},
            "schedule_interval": self.schedule_interval
        }
