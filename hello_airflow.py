# Airflow 3.x에서는
# DAG 작성용 공식 Public Interface로 airflow.sdk를 사용한다.
from airflow.sdk import dag, task

import pendulum


# =========================================================
# DAG 정의
# =========================================================

@dag(

    # Airflow 화면에서 보일 DAG 이름
    dag_id="hello_airflow",

    # schedule=None
    #
    # 아직 자동 스케줄은 사용하지 않고
    # 우리가 UI에서 직접 실행시키겠다는 의미
    schedule=None,

    # DAG가 실행 가능한 시작 시점
    #
    # pendulum은 timezone 처리가 편한
    # 날짜/시간 Python 라이브러리
    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="Asia/Seoul"
    ),

    # catchup=False
    #
    # 과거 실행 시점을 자동으로 밀린 만큼
    # 전부 소급 실행하지 않는다.
    catchup=False,

    tags=["practice"]
)
def hello_airflow():


    # =============================================
    # Task 정의
    # =============================================

    @task
    def say_hello():

        print(
            "Airflow Task 실행 성공!"
        )


    # DAG 안에 실제 Task 배치
    say_hello()


# DAG 객체 생성
hello_airflow()