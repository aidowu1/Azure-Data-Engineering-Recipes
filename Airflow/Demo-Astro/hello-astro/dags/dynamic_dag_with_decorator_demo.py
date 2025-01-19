import os
import time
import datetime as dt

import pendulum
from airflow.decorators import dag
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

def chunkCollection(data, n_chunks):
    def divideChunks(data, n_chunks): 
        
        # looping till length l 
        for i in range(0, len(data), n_chunks):  
            yield data[i:i + n_chunks] 
    return list(divideChunks(data, n_chunks))

DAG_ID = "dynamic_dag_etl_using_decorator"
SLEEP_TIME_SECS = 2
N_YEARS = 5
N_MONTHS = 12
START_YEAR = 2017
DAY = 1
TIME_SLOTS = [(dt.datetime.timestamp(dt.datetime(START_YEAR + year, month, DAY)), 
               dt.datetime.timestamp(dt.datetime(START_YEAR + year, month + 1, DAY)))
         for year in range(N_YEARS)
        for month in range(1, N_MONTHS)
        ]
CHUNKED_DATA = chunkCollection(TIME_SLOTS, N_YEARS)

def chunkData():
    """
    Chunk Datasets
    """    
    n_chunks = len(CHUNKED_DATA)    
    n_items_per_chunk = len(CHUNKED_DATA[0])
    print(f"Chunking data into {n_chunks} chunks")
    print(f"Number of data items per chunk is: {n_items_per_chunk}")
    time.sleep(SLEEP_TIME_SECS)


def ingestData():
    """
    Ingest data
    """
    print(f"Ingesting data..")
    time.sleep(SLEEP_TIME_SECS)

def upsertData():
    """
    Upsert data
    """
    print(f"Upserting data..")
    time.sleep(SLEEP_TIME_SECS)

def validateData():
    """
    Upsert data
    """
    print(f"Validating data..")
    time.sleep(SLEEP_TIME_SECS)

def xcomCleanup():
    """
    XCOM Cleanup
    """
    print(f"X-COM cleanup operation..")
    time.sleep(SLEEP_TIME_SECS)

def runDags(dag_id: str, chunk_id: int):

    @dag(
        dag_id=dag_id,
        start_date=pendulum.now(tz="Asia/Singapore"),
        schedule_interval=None,
        tags=["dynamic_etl_using_decorator"]
    )
    def create_dag():
    # with DAG(dag_id=dag_id,
    #          start_date=pendulum.now(tz="Asia/Singapore"),
    #          schedule_interval=None,
    #          tags=["chunk_etl"]
    #          ):
        
        chunk_data_task = PythonOperator(
            task_id='chunk_data_task',
            python_callable=chunkData
        )
        
        sources = CHUNKED_DATA[chunk_id]

        for i, source in enumerate(sources):
            group_id = f"Job-{str(i)}-{str(chunk_id)}"
            with TaskGroup(group_id=group_id) as task_group:
                ingest_data_task = PythonOperator(
                    task_id='ingest_data_task',
                    python_callable=ingestData
                )

                upsert_data_task = PythonOperator(
                    task_id='upsert_data_task',
                    python_callable=upsertData
                )

                validate_data_task = PythonOperator(
                    task_id='validate_data_task',
                    python_callable=validateData
                )

                xcomCleanup_task = PythonOperator(
                    task_id='xcomCleanup_task',
                    python_callable=xcomCleanup
                )            

                ingest_data_task >> upsert_data_task >> validate_data_task >> xcomCleanup_task 

            chunk_data_task >> task_group
    
    create_dag()    


# The env check is necessary to allow the unit test to mock the
# `get_sources()` function without having to invoke a connection
# to a real external database
#globals()[DAG_ID] = create_dag()
n_chunks = 3
for i in range(n_chunks):
    dag_id = f"{DAG_ID}_{str(i)}"
    runDags(dag_id=dag_id, chunk_id=i)