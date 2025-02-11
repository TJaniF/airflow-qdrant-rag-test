"""This DAG triggers reserialization of all DAGs in the /dags folder,
this means new DAGs will show up and existing DAGs are updated in the Airflow UI."""

from airflow.decorators import dag
from airflow.operators.bash import BashOperator


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
)
def dags_reserialize_helper():

    BashOperator(
        task_id="clean_airflow_metadata_db",
        bash_command="airflow dags reserialize",
    )


dags_reserialize_helper()
