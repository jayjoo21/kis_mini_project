from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from zoneinfo import ZoneInfo

from datetime import timedelta
import pendulum


POSTGRES_CONN_ID = "market_postgres"


@dag(
    dag_id="market_pipeline_health",

    # 평일 9~15시에 5분마다 실행
    schedule="*/5 9-15 * * 1-5",

    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="Asia/Seoul"
    ),

    catchup=False,

    tags=[
        "finance",
        "monitoring",
        "freshness"
    ]
)
def market_pipeline_health():


    @task
    def check_data_freshness():

        #현재 한국시간 확인
        now = pendulum.now("Asia/Seoul")

        current_time = now.time()

        market_open = pendulum.time(
            9,
            0,
            0
        )

        market_close = pendulum.time(
            15,
            30,
            0
        )

        print(
            "현재 한국시간:",
            now
        )

        #정규장 시간 밖이면 freshness 검사를 하지 x
        #장외시간에 새로운 tick이 없는 것은 장애가 아니라 정상적인 상태임

        if not(
            market_open
            <= current_time
            <= market_close
        ):

            raise AirflowSkipException(
                f"현재 시각 {current_time}은"
                "정규장 운영시간(09:00~15:30)이 아닙니다"
            )

        #정규장 시간 안
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        result = hook.get_first(
            """
            SELECT
                MAX(ingested_at),

                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP - MAX(ingested_at)
                    )
                )

            FROM market_price_events;
            """
        )


        latest_ingested_at = result[0]
        age_seconds = result[1]


        if latest_ingested_at is None:

            raise ValueError(
                "market_price_events에 데이터가 없습니다."
            )


        age_seconds = float(age_seconds)

        latest_ingested_at_kst = latest_ingested_at.astimezone(
            ZoneInfo("Asia/Seoul")
        )

        print(
            "최근 DB 적재 시각(KST):",
            latest_ingested_at_kst
        )

        print(
            "마지막 적재 이후 경과 시간:",
            f"{age_seconds:.1f}초"
        )


        # 실전용 기준은 우선 5분
        if age_seconds > 300:

            raise ValueError(
                f"Freshness 실패: "
                f"{age_seconds:.1f}초 동안 새로운 데이터가 없습니다."
            )


        print(
            "Freshness 정상"
        )


    check_data_freshness()


market_pipeline_health()