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
    default_args=default_args,
    schedule_interval=None,  # manual/external trigger
    catchup=False,
    tags=['embedding', 'langchain'],
)
def prompt_embedding_pipeline():

    @task
    def fetch_prompt() -> str:
        # Replace with message queue / db / api fetch logic
        user_prompt = "What is the future of AI?"
        return user_prompt

    @task
    def convert_prompt_to_embedding(user_prompt: str) -> List[float]:
        embedding_model = CohereEmbeddings(model="embed-english-v3.0")
        embedding = embedding_model.embed_query(user_prompt)
        return embedding

    @task
    def store_embedding(embedding: List[float]) -> None:
        # Replace this with actual DB/VectorDB storage
        print(f"Storing embedding: {embedding}")

    # Define task dependencies by function call chaining
    prompt = fetch_prompt()
    embedding = convert_prompt_to_embedding(prompt)
    store_embedding(embedding)

# Instantiate the DAG
prompt_embedding_pipeline()
