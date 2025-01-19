import random

from airflow.operators.empty import EmptyOperator
# from airflow.operators.dummy import DummyOperator
from airflow.models import DAG
from airflow.utils.dates import days_ago

with DAG(
   "big_dag",
   start_date=days_ago(1),
   schedule_interval=None,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for i in range(20):
        first = EmptyOperator(task_id=f"next_{i}")
        start >> first
        for j in range(random.randint(5, 15)):
            next = EmptyOperator(task_id=f"next_{i}_{j}")
            first >> next
            first = next
        next >> end