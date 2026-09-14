from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


# =========================================================
# Airflow → marketdb 연결 테스트 DAG
# =========================================================

@dag(
    dag_id="test_market_postgres",

    # 아직 자동 스케줄 없이 직접 실행
    schedule=None,

    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="Asia/Seoul"
    ),

    catchup=False,

    tags=["postgres", "practice"]
)
def test_market_postgres():


    @task
    def check_connection():

        # Airflow에 등록해둔
        # market_postgres Connection을 사용한다.
        #
        # DAG 코드 안에는
        # host / user / password를 직접 쓰지 않는다.
        hook = PostgresHook(
            postgres_conn_id="market_postgres"
        )


        # 현재 연결된 DB와 DB 사용자를 조회
        result = hook.get_first(
            """
            SELECT
                current_database(),
                current_user;
            """
        )


        print(
            "DB 연결 결과:",
            result
        )


        # 기대 결과:
        #
        # ('marketdb', 'kafkauser')
        if result[0] != "marketdb":

            raise ValueError(
                f"marketdb가 아닌 DB에 연결되었습니다: {result[0]}"
            )


    check_connection()


test_market_postgres()