import pendulum
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import  PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)


def aggregate():

    import glob
    import re
    import os

    from google.cloud import bigquery

    credentials_path="./data/service-account.json"

    client = bigquery.Client.from_service_account_json(credentials_path)

    data_path = "./logs/dag_id=DAG_aggregate_posts"
    log_types = ["INFO", "ERROR"]

    data = []

    for root, dirs, files in os.walk(data_path):
        for dir in dirs:
            folder_path = os.path.join(root, dir)
            info_logs_count = 0
            error_logs_count = 0

            log_file_path = os.path.join(folder_path, 'attempt=1.log')
            if os.path.exists(log_file_path):
                with open(log_file_path, 'r') as file:
                    lines = file.readlines()
                    for line in lines:
                        for log_type in log_types:
                            if re.search(rf'\b{log_type}\b', line):
                                if log_type == 'INFO':
                                    info_logs_count += 1
                                elif log_type == 'ERROR':
                                    error_logs_count += 1
            
                data.append({"log_type": "INFO", "count": info_logs_count})
                data.append({"log_type": "ERROR", "count": error_logs_count})
    
    dataset_id = "projet"

    dataset_ref = client.dataset(dataset_id)

    table_id_temp = "tempLogType"

    table_ref = dataset_ref.table(table_id_temp)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("log_type", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",  
    )

    job = client.load_table_from_json(
        data,
        table_ref,
        job_config=job_config,
    ) 

    job.result()

    query = """
    SELECT
        log_type as log_type,
        SUM(count) as count
    FROM
        `projet.tempLogType`  
    GROUP BY
        log_type
    """

 
    query_job = client.query(query)
    results = query_job.result() 

    data=[]

    for row in results:
        data.append({"log_type": row.log_type, "count": row.count})
        


    table_id = "countLogType"

    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("log_type", "STRING"),
            bigquery.SchemaField("count", "INTEGER"),
        ],
        write_disposition="WRITE_TRUNCATE",  
    )

    job = client.load_table_from_json(
        data,
        table_ref,
        job_config=job_config,
    )

    job.result()


with DAG(
    dag_id="DAG_aggregate_logs",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    virtual_classic = PythonVirtualenvOperator(
        task_id="aggregate_logs",
        requirements="google-cloud-bigquery",
        python_callable=aggregate,
    )

