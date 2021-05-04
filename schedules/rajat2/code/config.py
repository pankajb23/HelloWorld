


class Config(object):

    def __init__(self, fabric: str="common"):
        self.fabric = fabric
        self.dag_id = "rajat2"
        self.timezone = "US/Pacific"
        self.start_date = "2021-05-01T00:00:00"
        self.schedule_interval = "@daily"
        self.cluster_id = "j-2JURTB707CUAU"
        self.cluster_id = "eng.pankaj.002@prophecy.io_49_connId"

        if fabric == "emr":
            self.cluster_id = "j-2JURTB707CUAU"

    def dag_args(self):
        return {
            'dag_id': self.dag_id,
            'default_args': {
"depends_on_past" : False, "start_date" : self.start_date},
            "schedule_interval": self.schedule_interval
        }
