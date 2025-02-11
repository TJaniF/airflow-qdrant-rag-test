from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from pendulum import datetime

@dag(
    start_date=datetime(2025,1,1),
    schedule=[Dataset("features_test_a_DONE")],
    catchup=False,
)
def features_test_b():

    run_an_airflow_cli_command = BashOperator(
        task_id="run_an_airflow_cli_command",
        bash_command="airflow dags list",
    )

    @task 
    def write_a_file_to_include():
        with open("include/test.txt", "w") as f:
            f.write("Hello, World!")

    _write_a_file_to_include = write_a_file_to_include()

    @task
    def read_from_a_file_in_include():
        with open("include/test.txt", "r") as f:
            print(f.read())

    _read_from_a_file_in_include = read_from_a_file_in_include()

    chain(
        _write_a_file_to_include,
        _read_from_a_file_in_include,
    )


features_test_b()
