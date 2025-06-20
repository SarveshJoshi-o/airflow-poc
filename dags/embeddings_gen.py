from airflow.decorators import dag, task
from langchain_community.embeddings import CohereEmbeddings
from typing import List
import pendulum

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now("UTC").subtract(days=1),
}

# Define the DAG using the @dag decorator
@dag(
    dag_id='prompt_embedding_pipeline',
    default_args={
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
    },
    schedule=None,  # manual/external trigger
    tags=['embedding', 'langchain'],
)
def prompt_embedding_pipeline():

    @task
    def fetch_prompt() -> str:
        # Replace with message queue / db / api fetch logic
        user_prompt = "What is the future of AI?"
        return user_prompt

    _fetch_prompt = fetch_prompt()

    @task
    def convert_prompt_to_embedding(user_prompt: str) -> List[float]:
        embedding_model = CohereEmbeddings(model="embed-english-v3.0")
        embedding = embedding_model.embed_query(user_prompt)
        return embedding

    _convert_prompt_to_embedding = convert_prompt_to_embedding(_fetch_prompt)

    @task
    def store_embedding(embedding: List[float]) -> None:
        # Replace this with actual DB/VectorDB storage
        print(f"Storing embedding: {embedding}")

    # Define task dependencies by function call chaining

    store_embedding(_convert_prompt_to_embedding)

# Instantiate the DAG
prompt_embedding_pipeline()
