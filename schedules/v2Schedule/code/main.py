import json
from airflow import DAG

from graph import *
from config import Config


def main():
    config = load_config()
    with DAG(**config.dag_args()) as the_dag:
        print("Place your dag code here")


def load_config() -> Config:
    try:
        config = Config(**json.loads(configJson))
    except NameError:
        config = Config()

    return config


if __name__ == '__main__':
    main()

