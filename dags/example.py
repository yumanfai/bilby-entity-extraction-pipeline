from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="example",
) as dag:

    @task
    def example():
        print("Hello!")

        with open("output.txt", "w") as f:
            f.write("Hello!")

    example()
