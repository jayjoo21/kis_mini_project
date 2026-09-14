# =========================================================
# market_data_quality_daily.py
#
# 목적:
# PostgreSQL의 market_price_events 테이블에
# 실시간으로 쌓이고 있는 주식 Tick 데이터의 품질을 검사한다.
#
#
# 전체 실행 순서
#
# check_db_connection
#         ↓
# check_tick_count
#         ↓
# check_nulls
#         ↓
# check_invalid_price_volume
#         ↓
# daily_summary
#
#
# DQ = Data Quality
#    = 데이터 품질
# =========================================================


from airflow.sdk import dag, task

# PostgresHook:
# Airflow에 등록해 둔 PostgreSQL Connection을 이용해서
# SQL을 실행할 수 있게 해주는 객체
#
# 우리가 직접 psycopg.connect(
#     host=...,
#     user=...,
#     password=...
# )
# 를 쓰는 대신
#
# market_postgres라는 Airflow Connection을 사용한다.
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta
#time delta: 시간 차이: 몇 초 뒤에 재시도 할지

import pendulum


# =========================================================
# Airflow Connection ID
# =========================================================

# 앞에서 Airflow에 등록했던 Connection 이름
#
# 실제 연결 정보:
#
# host     = host.docker.internal
# database = marketdb
# user     = kafkauser
# port     = 5432
#
# 는 Airflow Metadata DB에 저장되어 있고,
# DAG에서는 Connection ID만 사용한다.
POSTGRES_CONN_ID = "market_postgres"


# =========================================================
# DAG 정의
# =========================================================

@dag(

    # Airflow UI에서 표시되는 DAG 이름
    dag_id="market_data_quality_daily",


    # 아직 학습 단계이므로 자동 실행하지 않는다.
    #
    # schedule=None
    # → Airflow UI에서 직접 Trigger해서 실행
    #
    # 나중에 장 마감 후 자동 실행으로 변경할 예정
    # 분 시 일 월 요일
    # 40 15 * * 1-5
    # → 월~금 15:40
    schedule="40 15 * * 1-5",


    # DAG가 실행 가능해지는 시작 시점
    #
    # Airflow는 내부적으로 UTC를 많이 사용하지만
    # 우리의 금융 데이터는 한국시장 데이터이므로
    # timezone을 Asia/Seoul로 명시한다.
    start_date=pendulum.datetime(
        2026,
        9,
        14,
        tz="Asia/Seoul"
    ),


    # catchup=False
    #
    # 과거 실행분을 자동으로 몰아서 실행하지 않는다.
    #
    # 예를 들어 매일 실행되는 DAG를 오늘 처음 만들었다고 해서
    # 지난 날짜의 DAG Run을 전부 생성하지 않게 한다.
    catchup=False,

    # 각 Task가 실패하면 최대 2번 재시도
    default_args={
        "retries": 2,

        # 재시도 사이에 5분 대기
        "retry_delay": timedelta(minutes=5),
    },


    tags=[
        "finance",
        "data-quality",
        "postgres"
    ]
)
def market_data_quality_daily():


    # =====================================================
    # TASK 1
    # PostgreSQL 연결 확인
    # =====================================================

    @task
    def check_db_connection():

        # market_postgres Connection을 이용해서
        # PostgreSQL에 접속하는 Hook 생성
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )


        # get_first()
        #
        # SQL 실행 결과 중 첫 번째 행을 가져온다.
        #
        # 아래 SQL 결과는:
        #
        # ('marketdb', 'kafkauser')
        #
        # 형태가 된다.
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


        # 혹시 marketdb가 아니라
        # 다른 PostgreSQL DB에 연결된 경우
        # Airflow Task를 실패 처리한다.
        if result[0] != "marketdb":

            raise ValueError(
                f"잘못된 DB에 연결됨: {result[0]}"
            )



    # =====================================================
    # TASK 2
    # 오늘 들어온 Tick 데이터 건수 검사
    # =====================================================

    @task
    def check_tick_count():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )


        # ingested_at:
        #
        # Tick Event가 실제로 우리 PostgreSQL에
        # 저장된 시각
        #
        # "오늘 데이터가 실제로 수집되고 있는가?"
        # 를 검사하는 것이므로
        # event_time보다 ingested_at을 기준으로 검사한다.
        #
        #
        # AT TIME ZONE 'Asia/Seoul'
        #
        # PostgreSQL의 시간값을
        # 한국시간 기준으로 해석한다.
        #
        #
        # ::date
        #
        # timestamp에서 날짜 부분만 뽑는다.
        #
        # 예:
        #
        # 2026-09-14 11:20:35
        #
        # ↓ ::date
        #
        # 2026-09-14

        count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM market_price_events
            WHERE
                (ingested_at AT TIME ZONE 'Asia/Seoul')::date
                =
                (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul')::date;
            """
        )[0]


        print(
            "오늘 적재된 Tick 데이터 건수:",
            count
        )


        # 0건이라면:
        #
        # WebSocket이 꺼졌거나
        # Kafka Consumer가 꺼졌거나
        # DB INSERT가 실패했을 가능성이 있으므로
        # 정상 상태로 보지 않는다.
        if count == 0:

            raise ValueError(
                "오늘 적재된 Tick 데이터가 없습니다."
            )

    # =====================================================
    # TASK 3
    # 데이터 Freshness 검사
    # =====================================================

    @task(
            retries=2,
            retry_delay=timedelta(seconds=30)
    )

    def check_data_freshness():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        # -------------------------------------------------
        # Freshness
        # = 데이터가 얼마나 "최신 상태"인지 검사
        #
        # MAX(ingested_at)
        # → PostgreSQL에 가장 최근에 적재된 시각
        #
        # CURRENT_TIMESTAMP
        # → 현재 PostgreSQL 서버 시각
        #
        # 둘의 차이를 초(second) 단위로 계산한다.
        #
        # EXTRACT(EPOCH FROM ...)
        #
        # EPOCH:
        # 시간을 초 단위 숫자로 표현할 때 사용하는 방식
        # -------------------------------------------------

        result = hook.get_first(
            """
            SELECT
                MAX(ingested_at) AS latest_ingested_at,

                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP - MAX(ingested_at)
                    )
                ) AS age_seconds

            FROM market_price_events;
            """
        )


        latest_ingested_at = result[0]
        age_seconds = result[1]


        print(
            "가장 최근 DB 적재 시각:",
            latest_ingested_at
        )

        print(
            "현재 시각과의 차이(초):",
            age_seconds
        )


        # 데이터가 아예 없으면
        # Freshness를 검사할 수 없으므로 실패
        if latest_ingested_at is None:

            raise ValueError(
                "market_price_events에 데이터가 없습니다."
            )


        # Decimal 등의 숫자 타입을
        # Python float으로 변환
        age_seconds = float(age_seconds)


        # -------------------------------------------------
        # 우선 실습을 위해 60초로 설정
        #
        # 최근 DB 적재가 60초 이상 멈췄으면
        # 데이터 수집 파이프라인 이상으로 판단
        #
        # 실습 후에는 300초(5분) 정도로
        # 변경할 예정
        # -------------------------------------------------

        if age_seconds > 60:

            raise ValueError(
                f"데이터 Freshness 실패: "
                f"최근 데이터가 {age_seconds:.1f}초 전에 적재되었습니다."
            )


        print(
            "Freshness 검사 성공:",
            f"{age_seconds:.1f}초 전 데이터까지 존재"
        )   

    # =====================================================
    # TASK 4
    # 핵심 Column NULL 검사
    # =====================================================

    @task
    def check_nulls():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )


        # NULL:
        #
        # 값이 0이라는 뜻이 아니라
        # "값 자체가 존재하지 않음"이라는 뜻
        #
        # Tick Event에 반드시 있어야 하는:
        #
        # symbol
        # price
        # volume
        # event_time
        #
        # 중 하나라도 NULL인 데이터가 있는지 검사한다.
        #
        # 현재는 오늘 DB에 적재된 데이터만 검사한다.

        null_count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM market_price_events
            WHERE
                (
                    symbol IS NULL
                    OR price IS NULL
                    OR volume IS NULL
                    OR event_time IS NULL
                )
                AND
                (
                    ingested_at AT TIME ZONE 'Asia/Seoul'
                )::date
                =
                (
                    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'
                )::date;
            """
        )[0]


        print(
            "NULL 데이터 건수:",
            null_count
        )


        # NULL이 하나라도 발견되면
        # 데이터 품질 검사 실패
        if null_count > 0:

            raise ValueError(
                f"NULL 데이터 발견: {null_count}건"
            )



    # =====================================================
    # TASK 4
    # 가격 / 거래량 논리 검사
    # =====================================================

    @task
    def check_invalid_price_volume():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )


        # 정상 주식 체결 데이터라면:
        #
        # price > 0
        # volume >= 0
        #
        # 이어야 한다.
        #
        # 따라서:
        #
        # price <= 0
        # volume < 0
        #
        # 인 데이터는 비정상 데이터로 판단한다.

        invalid_count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM market_price_events
            WHERE
                (
                    price <= 0
                    OR volume < 0
                )
                AND
                (
                    ingested_at AT TIME ZONE 'Asia/Seoul'
                )::date
                =
                (
                    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'
                )::date;
            """
        )[0]


        print(
            "비정상 가격 / 거래량 데이터 건수:",
            invalid_count
        )


        if invalid_count > 0:

            raise ValueError(
                f"비정상 가격/거래량 데이터 발견: {invalid_count}건"
            )



    # =====================================================
    # TASK 5
    # 오늘 시장 데이터 간단 요약
    # =====================================================

    @task
    def daily_summary():

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        # -----------------------------------------------------
        # Raw Tick 데이터를
        # 날짜 + 종목 단위로 집계해서
        # market_daily_summary Mart 테이블에 저장한다.
        #
        # GROUP BY:
        # 같은 날짜 + 같은 종목끼리 하나로 묶는다.
        #
        # 예:
        #
        # 005930 / 09:00 / 250000
        # 005930 / 09:01 / 250500
        # 005930 / 09:02 / 251000
        #
        # ↓ GROUP BY
        #
        # 2026-09-14 / 005930 / 하루 요약 1행
        # -----------------------------------------------------

        sql = """
            INSERT INTO market_daily_summary (
                trade_date,
                symbol,
                tick_count,
                first_event_time,
                last_event_time,
                min_price,
                max_price,
                total_volume,
                updated_at
            )

            SELECT
                (
                    event_time AT TIME ZONE 'Asia/Seoul'
                )::date AS trade_date,

                symbol,

                COUNT(*) AS tick_count,

                MIN(event_time) AS first_event_time,

                MAX(event_time) AS last_event_time,

                MIN(price) AS min_price,

                MAX(price) AS max_price,

                SUM(volume) AS total_volume,

                NOW() AS updated_at

            FROM market_price_events

            WHERE
                (
                    event_time AT TIME ZONE 'Asia/Seoul'
                )::date
                =
                (
                    CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul'
                )::date

            GROUP BY
                (
                    event_time AT TIME ZONE 'Asia/Seoul'
                )::date,
                symbol


            ON CONFLICT (trade_date, symbol)

            DO UPDATE SET

                tick_count =
                    EXCLUDED.tick_count,

                first_event_time =
                    EXCLUDED.first_event_time,

                last_event_time =
                    EXCLUDED.last_event_time,

                min_price =
                    EXCLUDED.min_price,

                max_price =
                    EXCLUDED.max_price,

                total_volume =
                    EXCLUDED.total_volume,

                updated_at =
                    NOW();
        """

        hook.run(sql)

        print(
            "market_daily_summary 저장 완료"
        )



    # =====================================================
    # Task 생성
    # =====================================================

    db_task = check_db_connection()

    count_task = check_tick_count()

    #freshness_task = check_data_freshness()

    null_task = check_nulls()

    invalid_task = check_invalid_price_volume()

    summary_task = daily_summary()



    # =====================================================
    # Task Dependency 설정
    # =====================================================

    # Dependency
    # = 의존 관계
    #
    # >> 연산자는:
    # 왼쪽 Task가 성공해야
    # 오른쪽 Task를 실행한다.

    # 따라서 최종 실행 순서는:
    #
    # DB 연결 확인
    #       ↓
    # 데이터 존재 확인
    #       ↓
    # NULL 검사
    #       ↓
    # 가격 / 거래량 검사
    #       ↓
    # 일별 데이터 요약

    (
        db_task
        >> count_task
        #>> freshness_task
        >> null_task
        >> invalid_task
        >> summary_task
    )


# =========================================================
# DAG 객체 생성
# =========================================================

market_data_quality_daily()