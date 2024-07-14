"""
This DAG is demonstrating a simple task to read data from a Firestore collection.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from firebase.firestore_config import client


def read_firestore_data(**kwargs):
    collection = client.collection('automation_catalog')
    docs = collection.stream()
    for doc in docs:
        print(f'{doc.id} => {doc.to_dict()}')

with DAG(
    dag_id="simple_db_read",
    default_args={"retries": 0},
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    dag.doc_md = __doc__

    read_op = PythonOperator(
        task_id="read_firestore_data",
        python_callable=read_firestore_data,
    )

    read_op
