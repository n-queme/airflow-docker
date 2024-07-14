"""
### DAG Tutorial Documentation
This DAG is demonstrating an Extract -> Transform -> Load pipeline
"""

from __future__ import annotations

import json, pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


# [START instantiate_dag]
with DAG(
    dag_id="simple_etl",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2},
    # [END default_args]
    description="DAG tutorial",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]


    # [START core_functions]
    def extract(**kwargs):
        ti = kwargs["ti"]
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("order_data", data_string)

    def transform(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="extract", key="order_data")
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push("total_order_value", total_value_json_string)

    def load(**kwargs):
        ti = kwargs["ti"]
        total_value_string = ti.xcom_pull(task_ids="transform", key="total_order_value")
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END core_functions]


    # [START main_flow]
    extract_op = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_op = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_op = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_op >> transform_op >> load_op