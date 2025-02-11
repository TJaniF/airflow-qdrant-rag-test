"""
## RAG test DAG
"""

import logging
import os

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

# Variables used in the DAG
_DB_DIRECTORY = os.getenv("DB_DIRECTORY", "include/qdrant_local_db")
_COLLECTION_NAME = os.getenv("COLLECTION_NAME", "my_books_collection")
_EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL", "colbert-ir/colbertv2.0"
)  # other option: answerdotai/answerai-colbert-small-v1 (96 dim)


@dag(
    dag_display_name="🔍 Query Book Knowledge",
    start_date=datetime(2025, 1, 1),
    schedule=Dataset("qdrant://my_books_collection"),
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1, # To prevent parallel requests to the local Qdrant database
    tags=["Query"],
    default_args={
        "retries": 0,
        "retry_delay": duration(seconds=10),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Ingest book information into a vector database for RAG.",
    params={
        "query_string": Param(
            "A philosophical book",
            type="string",
            title="Book Mood To Query for",
            description="Enter a description of the type of book you'd like to read.",
            minLength=1,
            maxLength=200,
        )
    },
)
def query_data():

    @task
    def query_db(
        db_directory: str = _DB_DIRECTORY,
        collection_name: str = _COLLECTION_NAME,
        embedding_model: str = _EMBEDDING_MODEL,
        **context
    ):
        from fastembed import LateInteractionTextEmbedding
        from qdrant_client import QdrantClient

        embedding_model = LateInteractionTextEmbedding(embedding_model)

        qdrant_client = QdrantClient(path=db_directory)

        query_string = context["params"]["query_string"]

        r = qdrant_client.query_points(
            collection_name=collection_name,
            query=list(embedding_model.query_embed(query_string))[0],
            limit=1,
            with_payload=True,
        )

        return r

    query_db()


query_data()
