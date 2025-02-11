"""
## RAG test DAG
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import os
import logging
import pandas as pd
from airflow.datasets import Dataset

t_log = logging.getLogger("airflow.task")

# Variables used in the DAG

_BOOK_DESCRIPTION_FOLDER = os.getenv("BOOK_DESCRIPTION_FOLDER", "include/data")
_DB_DIRECTORY = os.getenv("DB_DIRECTORY", "include/qdrant_local_db")
_COLLECTION_NAME = os.getenv("COLLECTION_NAME", "my_books_collection")
_VECTOR_SIZE = int(os.getenv("VECTOR_SIZE", 128))
_EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL", "colbert-ir/colbertv2.0"
)  # other option: answerdotai/answerai-colbert-small-v1 (96 dim)


@dag(
    dag_display_name="📚 Ingest Book Knowledge",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    max_active_runs=1, # To prevent parallel requests to the local Qdrant database
    tags=["RAG"],
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=10),
        "owner": "AI Task Force",
    },
    doc_md=__doc__,
    description="Ingest book information into a vector database for RAG.",
)
def fetch_data():

    @task(retries=4)
    def prepare_vector_db(
        db_directory: str = _DB_DIRECTORY,
        collection_name: str = _COLLECTION_NAME,
        vector_size: int = _VECTOR_SIZE,
    ):
        """
        Prepare the vector database.
        """
        from qdrant_client import QdrantClient, models

        client = QdrantClient(path=db_directory)

        client.recreate_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(
                size=vector_size,
                distance=models.Distance.COSINE,
                multivector_config=models.MultiVectorConfig(
                    comparator=models.MultiVectorComparator.MAX_SIM
                ),
            ),
        )

        collections = client.get_collections()

        print(f"Available collections: {collections}")

        # turn collections into dict
        collections_names_list = [
            collection_description.name
            for collection_description in collections.collections
        ]

        return collections_names_list

    _prepare_vector_db = prepare_vector_db()

    @task
    def list_book_description_files(
        book_description_folder: str = _BOOK_DESCRIPTION_FOLDER,
    ):
        """
        List the files in the book description folder.
        """
        book_description_files = os.listdir(book_description_folder)
        return book_description_files

    _list_book_description_files = list_book_description_files()

    @task(
        map_index_template="{{ my_custom_map_index }}",
    )
    def read_book_description_files(
        book_description_file: list,
        book_description_folder: str = _BOOK_DESCRIPTION_FOLDER,
    ):
        """
        Read the book description files.
        """
        with open(
            os.path.join(book_description_folder, book_description_file), "r"
        ) as f:
            book_descriptions = f.readlines()

        book_ids = [
            book_description.split(":::")[0].strip()
            for book_description in book_descriptions
        ]
        titles = [
            book_description.split(":::")[1].strip()
            for book_description in book_descriptions
        ]
        authors = [
            book_description.split(":::")[2].strip()
            for book_description in book_descriptions
        ]
        book_description_text = [
            book_description.split(":::")[3].strip()
            for book_description in book_descriptions
        ]

        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Reading file {book_description_file}."


        # create one dict per book
        book_descriptions = [
            {
                "source_path": f"{book_description_folder}/{book_description_file}",
                "book_id": book_id,
                "title": title,
                "author": author,
                "description": description,
            }
            for book_id, title, author, description in zip(
                book_ids, titles, authors, book_description_text
            )
        ]

        return book_descriptions

    _read_book_description_files = read_book_description_files.expand(
        book_description_file=_list_book_description_files
    )

    @task
    def create_vector_embeddings(
        book_data: list, embedding_model: str = _EMBEDDING_MODEL
    ):
        from fastembed import LateInteractionTextEmbedding

        book_descriptions = [book["description"] for book in book_data]

        print(book_descriptions)

        embedding_model = LateInteractionTextEmbedding(embedding_model)

        description_embeddings = [
            embedding.tolist() for embedding in embedding_model.embed(book_descriptions)
        ]

        return description_embeddings

    _create_vector_embeddings = create_vector_embeddings.expand(
        book_data=_read_book_description_files
    )

    @task
    def flatten_lists_of_embeddings(lists_of_embeddings: list):
        print(lists_of_embeddings)
        return [item for sublist in lists_of_embeddings for item in sublist]

    _flatten_lists_of_embeddings = flatten_lists_of_embeddings(
        _create_vector_embeddings
    )

    @task(outlets=[Dataset("qdrant://my_books_collection")])
    def load_to_qdrant(
        book_description_files: list,
        book_embeddings: list,
        db_directory: str = _DB_DIRECTORY,
        collection_name: str = _COLLECTION_NAME,
    ):
        from qdrant_client import QdrantClient, models

        qdrant_client = QdrantClient(path=db_directory)

        book_data_list = [item for sublist in book_description_files for item in sublist]



        qdrant_client.upload_points(
            collection_name=collection_name,
            points=[
                models.PointStruct(
                    id=int(book_data["book_id"]),
                    payload={
                        "book_id": int(book_data["book_id"]),
                        "author": book_data["author"],
                        "title": book_data["title"],
                    },
                    vector=vector,
                ) 
                for book_data, vector in zip(book_data_list, book_embeddings)
            ],
        )

    _load_to_qdrant = load_to_qdrant(
        book_description_files=_read_book_description_files,
        book_embeddings=_flatten_lists_of_embeddings,
    )

    chain(
        _prepare_vector_db,
        _list_book_description_files,
        _read_book_description_files,
        _load_to_qdrant,
    )


fetch_data()
