from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
import os

_DB_DIRECTORY = os.getenv("DB_DIRECTORY", "include/qdrant_local_db")


@dag(
    start_date=None,
    schedule=None,
    catchup=False,
)
def cleanup_helper():

    @task
    def delete_qdrant_db(db_directory: str = _DB_DIRECTORY):
        """
        Delete the Qdrant database.
        Args:
            db_directory (str): The directory where the Qdrant database is stored.
        """
        import shutil

        if os.path.exists(db_directory):
            shutil.rmtree(db_directory)
            print(f"Deleted Qdrant database at {db_directory}")
        else:
            print(f"No Qdrant database found at {db_directory}")

    delete_qdrant_db()

    BashOperator(
        task_id="clean_airflow_metadata_db_xcom",
        bash_command="airflow db clean --clean-before-timestamp {{ ts }} --tables xcom --skip-archive --verbose  --yes ",
    )


cleanup_helper()
