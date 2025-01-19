from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from typing import List


params = {
    "number_of_tasks": 10,
}

