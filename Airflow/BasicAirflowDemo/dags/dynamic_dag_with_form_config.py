from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


def dynamic_task(task_id, **kwargs):
    print(f"Executing task: {task_id}")
    params = kwargs['dag_run'].conf  # Retrieve parameters passed via the Airflow UI
    print(f"Parameters for this task: {params}")


with DAG(
    dag_id='dynamic_taskgroup_dag',
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dynamic', 'taskgroup'],
    params={
        "number_of_tasks": 3
    },
) as dag:

    def create_dynamic_tasks(number_of_tasks, task_group):
        """Creates a dynamic number of tasks within the task group."""
        for i in range(1, number_of_tasks + 1):
            task = PythonOperator(
                task_id=f'start_task_{i}',
                python_callable=dynamic_task,
                op_kwargs={'task_id': f'start_task_{i}'},
                task_group=task_group,  # Assign to the TaskGroup
                provide_context=True,
            )
            task

    # Parent task to determine parameters
    def parse_input(**kwargs):
        params = kwargs['dag_run'].conf or {}  # Get the parameters
        number_of_tasks = params.get('number_of_tasks', 5)  # Default to 5 tasks if not provided
        print(f"Number of tasks to create: {number_of_tasks}")
        return number_of_tasks

    # Define a task to parse the input
    parse_input_task = PythonOperator(
        task_id='parse_input',
        python_callable=parse_input,
        provide_context=True,
    )

    # Dynamically create tasks inside a TaskGroup
    with TaskGroup(group_id='dynamic_tasks_group') as dynamic_group:
        # PythonOperator to dynamically generate tasks
        generate_tasks_task = PythonOperator(
            task_id='generate_tasks',
            python_callable=lambda **kwargs: create_dynamic_tasks(
                number_of_tasks=kwargs['ti'].xcom_pull(task_ids='parse_input'),
                task_group=dynamic_group,
            ),
            provide_context=True,
        )

    # Task dependencies
    parse_input_task >> generate_tasks_task



