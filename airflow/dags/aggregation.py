import pendulum
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import  PythonVirtualenvOperator, is_venv_installed
from datetime import timedelta

log = logging.getLogger(__name__)


def aggregate():
    from google.cloud import bigquery

    from pymongo import MongoClient
    client = MongoClient('mongodb://mongo:mongo@mongoDB:27017/')
    db = client['airflow'] 
    collection = db['posts'] 
    credentials_path="./data/service-account.json"

    client = bigquery.Client.from_service_account_json(credentials_path)
    
    data= collection.aggregate([
        {
            "$group": {
                "_id": "$@OwnerUserId",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ])

    
    dataset_id = "projet"

    dataset_ref = client.dataset(dataset_id)

    table_id = "postsByAuthor"

    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("_id", "INTEGER"),
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
    
    data = collection.aggregate([
        {
            "$group": {
                "_id": "$@PostTypeId",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ])

    table_id = "postsByType"

    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("_id", "INTEGER"),
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

    data = collection.aggregate([
    {
        "$group": {
            "_id": "$@OwnerUserId",
            "total_score": { "$sum": { "$toInt": "$@Score" } }
        }
    },
    {
        "$sort": {"total_score": -1}
    }
    ])    

    table_id = "scoreByAuthor" 

    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("_id", "INTEGER"),
            bigquery.SchemaField("total_score", "INTEGER"),
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
    dag_id="DAG_aggregate_posts",
    schedule=timedelta(minutes=30),
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    tags=[],
) as dag:
    
    if not is_venv_installed():
        log.warning("The virtalenv_python example task requires virtualenv, please install it.")

    virtual_classic = PythonVirtualenvOperator(
        task_id="aggregate",
        requirements=["pymongo","google-cloud-bigquery"],
        python_callable=aggregate,
    )

