from airflow.decorators import dag, task_group, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from airflow.models.param import Param


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
    params={
        "my_string": Param(
            "Hello :)",
            type="string",
            title="A string to print in a task",
            description="Enter any string between 1 and 200 characters",
            minLength=1,
            maxLength=200,
        )
    },
)
def features_test_a():

    @task
    def get_data_from_public_api():
        import requests

        r = requests.get("https://manateejokesapi.herokuapp.com//manatees/random")

        print(r.json())

    get_data_from_public_api()

    @task
    def get_list():
        return [1, 2, 3, 4, 5]

    @task(
        map_index_template="{{ my_custom_map_index }}"
    )
    def dynamic_task_mapping(num):
        num_cubed = num ** 3
        print(num_cubed)

        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"{num_cubed} is the cube of {num}!"

    dynamic_task_mapping.expand(num=get_list())

    @task.branch
    def branch_task():
        import random

        random_number = random.randint(0, 10)
        if random_number > 5:
            return "task_a"
        else:
            return "task_b"

    _task_a = EmptyOperator(task_id="task_a")
    _task_b = EmptyOperator(task_id="task_b")

    _downstream_task = EmptyOperator(
        task_id="downstream_task", trigger_rule="none_failed"
    )

    chain(branch_task(), [_task_a, _task_b], _downstream_task)

    @task(outlets=Dataset("features_test_a_DONE"))
    def produce_to_dataset(**context):
        import random

        random_number = random.randint(0, 10)

        # this should be shown in the UI with the dataset event
        context["outlet_events"][Dataset("features_test_a_DONE")].extra = {
            "my_num": random_number
        }

    produce_to_dataset()

    @task
    def print_param(**context):
        print(context["params"]["my_string"])

    print_param()

    @task_group
    def my_task_group():

        @task
        def task_d():
            print("Task d")

        @task
        def task_e():
            print("Task e")

        chain(task_d(), task_e())

    my_task_group()


features_test_a()
