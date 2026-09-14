from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="hello_airflow",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="Asia/Seoul"
    ),
    catchup=False,
    tags=["practice"]
)
def hello_airflow():

    @task
    def say_hello():
        print("Airflow Task 실행 성공!")

    say_hello()


hello_airflow()