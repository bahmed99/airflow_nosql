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
    import json
    import random

    from google.cloud import bigquery

    credentials_path="./data/service-account.json"

    client = bigquery.Client.from_service_account_json(credentials_path)

    data_filepath = "./data/movies-stackexchange/json/Badges.json"

    with open(data_filepath, "r") as f:
        content = f.read()
        badges = json.loads(content)
        badge = random.choice(badges)

    
   
    
    dataset_id = "projet"

    dataset_ref = client.dataset(dataset_id)

    table_id_badges = "Badges"

    table_ref = dataset_ref.table(table_id_badges)

    query_job = client.query(
        f"SELECT Id FROM `{dataset_id}.{table_id_badges}` WHERE Id = '{badge['Id']}'"
    )
    results = query_job.result()

    if(list(results)):
        return
    badges_to_insert = [badge]


    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Id", "STRING"),
            bigquery.SchemaField("UserId", "STRING"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Date", "STRING"),
            bigquery.SchemaField("Class", "STRING"),
            bigquery.SchemaField("TagBased", "STRING")
        ],
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_json(
        badges_to_insert,
        table_ref,
        job_config=job_config,
    ) 

    job.result()

    query = """
    SELECT
        Name as name,
        count(*) as count
    FROM
        `projet.Badges`  
    GROUP BY
        name
    """

 
    query_job = client.query(query)
    results = query_job.result() 

    data=[]

    for row in results:
        data.append({"name": row.name, "count": row.count})
        


    table_id = "countBadgeName"

    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
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
    dag_id="DAG_aggregate_badges",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    virtual_classic = PythonVirtualenvOperator(
        task_id="aggregate_badges",
        requirements="google-cloud-bigquery",
        python_callable=aggregate,
    )

