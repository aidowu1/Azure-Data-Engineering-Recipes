"""DAG demonstrating the umbrella use case with dummy operators."""

import airflow.utils.dates
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PARAMS = {
    "number_of_tasks": 10
}


def getParamsFromForm(**kwargs):
    params = kwargs['dag_run'].conf or {}  # Get the parameters
    number_of_tasks = params.get('number_of_tasks', 5)  # Default to 5 tasks if not provided
    print(f"Number of tasks to create: {number_of_tasks}")
    Variable.set("number_of_tasks", int(number_of_tasks) + 1)
    return number_of_tasks

def getParamsFromVariable(**kwargs):
    number_of_tasks = Variable.get("number_of_tasks")
    product_ids = Variable.get("product_ids")
    print(f"Number of tasks stored in environment variable: {number_of_tasks}")
    print(f"Product IDs stored in environment variable: {product_ids}")

dag1 = DAG(
    dag_id="config_param_form",
    description="Configuration parameters for Airflow ETL",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
    params=PARAMS,
)
begin = EmptyOperator(task_id="begin_task", dag=dag1)
get_params_from_form = task = PythonOperator(
                task_id='get_params_from_form',
                python_callable=getParamsFromForm,
                provide_context=True,
                dag=dag1
            )
controller_trigger = TriggerDagRunOperator(
        task_id="controller_trigger",
        trigger_dag_id="01_umbrella",
        conf={},
        dag=dag1,
    )
end = EmptyOperator(task_id="end_task", dag=dag1)

# Specify dependencies for setting config params from UI (form)
begin >> get_params_from_form >> controller_trigger >> end


dag2 = DAG(
    dag_id="01_umbrella",
    description="Umbrella example with DummyOperators.",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@daily",
)
get_params_from_variable = PythonOperator(
                task_id='get_params_from_variable',
                python_callable=getParamsFromVariable,
                provide_context=True,
                dag=dag2
            )

fetch_weather_forecast = EmptyOperator(task_id="fetch_weather_forecast", dag=dag2)
fetch_sales_data = EmptyOperator(task_id="fetch_sales_data", dag=dag2)
clean_forecast_data = EmptyOperator(task_id="clean_forecast_data", dag=dag2)
clean_sales_data = EmptyOperator(task_id="clean_sales_data", dag=dag2)
join_datasets = EmptyOperator(task_id="join_datasets", dag=dag2)
train_ml_model = EmptyOperator(task_id="train_ml_model", dag=dag2)
deploy_ml_model = EmptyOperator(task_id="deploy_ml_model", dag=dag2)

# Set dependencies between ETL tasks
fetch_weather_forecast >> clean_forecast_data
fetch_sales_data >> clean_sales_data
[clean_forecast_data, clean_sales_data] >> join_datasets
get_params_from_variable >> join_datasets >> train_ml_model >> deploy_ml_model
