"""
This DAG is demonstrating a simple task to read data from a Firestore collection.
Fetch data from an API.
And execute one of two tasks based on the response from the API.
"""

from __future__ import annotations

import json
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from firebase.firestore import Firestore


def read_firestore_data(**kwargs):
    docs = Firestore.scan('automation_catalog')
    doc_count = len(docs)
    for doc in docs:
        print(f'Doc => {doc}')
    return doc_count  # Return the number of documents


def log_response(**context):
    ti = context['task_instance']
    response = ti.xcom_pull(task_ids='post_request')
    print(f'HTTP Response: {response}')
    return response


def choose_branch(**context):
    ti = context['task_instance']
    response = ti.xcom_pull(task_ids='log_response')
    response_json = json.loads(response)
    if response_json and 'id' in response_json and response_json['id'] > 100:
        return 'more_than_hundred'
    else:
        return 'less_than_hundred'


def check_id_more_than_hundred(**context):
    print('ID is more than 100')


def check_id_less_than_hundred(**context):
    print('ID is less than 100')


with DAG(
    dag_id="db_and_http",
    default_args={"retries": 0},
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["firebase", "http"],
) as dag:

    read_op = PythonOperator(
        task_id="read_firestore_data",
        python_callable=read_firestore_data,
    )

    post_op = SimpleHttpOperator(
        task_id="post_request",
        method='POST',
        http_conn_id='api_conn',  
        endpoint='posts',
        data="""{"title": "foo", "body": "bar", "userId": {{ ti.xcom_pull(task_ids='read_firestore_data') | int }}}""",
        headers={"Content-Type": "application/json"},
        log_response=True,
    )

    log_op = PythonOperator(
        task_id="log_response",
        python_callable=log_response,
        provide_context=True,
    )

    branch_op = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branch,
        provide_context=True,
    )

    more_than_hundred_op = PythonOperator(
        task_id='more_than_hundred',
        python_callable=check_id_more_than_hundred,
    )

    less_than_hundred_op = PythonOperator(
        task_id='less_than_hundred',
        python_callable=check_id_less_than_hundred,
    )

    read_op >> post_op >> log_op >> branch_op
    branch_op >> [more_than_hundred_op, less_than_hundred_op]
