from confluent_kafka import Consumer
import json
import psycopg
from datetime import datetime

# =========================================================
# 1. Kafka Consumer 생성
# =========================================================

consumer = Consumer({

    # Kafka Broker 주소
    "bootstrap.servers": "localhost:9092",

    # Consumer Group
    #
    # Kafka는 Group별로
    # "어디까지 메시지를 읽었는지" Offset을 관리한다.
    #
    # 따라서 프로그램을 껐다 다시 실행하더라도
    # 동일한 Group이면 일반적으로 이전에 읽던 위치부터 이어간다.
    "group.id": "market-tick-db-group-v2",

    # 이 Consumer Group에 저장된 Offset 정보가 없을 때
    # Topic의 가장 처음 메시지부터 읽는다.
    #
    # earliest = 가장 이른 위치
    "auto.offset.reset": "earliest"
})


# 실시간 체결 Event가 들어오는 Topic 구독
consumer.subscribe([
    "market-tick-events-v2"
])


# =========================================================
# 2. PostgreSQL 연결
# =========================================================

# psycopg:
# Python에서 PostgreSQL과 통신할 수 있게 해주는 라이브러리
conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="marketdb",
    user="kafkauser",
    password="kafkapass"
)


# cursor:
# PostgreSQL에 SQL 명령을 전달하는 객체
cursor = conn.cursor()


print("Waiting for realtime tick events...")


# =========================================================
# 3. Kafka Event 계속 수신
# =========================================================

try:

    while True:

        # Kafka에 새 메시지가 있는지 최대 1초 동안 확인
        #
        # poll = 조회하다 / 확인하다
        msg = consumer.poll(1.0)


        # 1초 동안 새 Event가 없었다면
        # 다시 while문의 처음으로
        if msg is None:
            continue


        # Kafka 자체 오류가 있는 경우
        if msg.error():

            print(
                "Kafka error:",
                msg.error()
            )

            continue


        # =================================================
        # 4. Kafka bytes → 문자열
        # =================================================

        # Kafka Message Value는 Byte 형태로 들어온다.
        #
        # decode("utf-8")
        # Byte → 일반 Python 문자열
        raw_value = msg.value().decode(
            "utf-8"
        )


        # =================================================
        # 5. JSON 문자열 → Python Dictionary
        # =================================================

        try:

            # json.loads()
            #
            # JSON 문자열:
            # '{"symbol":"005930", ...}'
            #
            # ↓
            #
            # Python dict:
            # {
            #     "symbol": "005930",
            #     ...
            # }
            event = json.loads(
                raw_value
            )


        except json.JSONDecodeError:

            print(
                "JSON 오류:",
                raw_value
            )

            continue


        # =================================================
        # 6. 기본 Data Quality 검사
        # =================================================

        # 가격은 0 이하일 수 없으므로
        # 이상 데이터라면 DB에 저장하지 않는다.
        if event["price"] <= 0:

            print(
                "DQ FAIL - price:",
                event
            )

            continue


        # 거래량 역시 음수일 수 없다.
        if event["volume"] < 0:

            print(
                "DQ FAIL - volume:",
                event
            )

            continue

        try:
            datetime.fromisoformat(
                event["event_time"]
            )

        except (ValueError, TypeError):

            print(
                "DQ FAIL - event_time:",
                event
            )

            continue

        # =================================================
        # 7. PostgreSQL INSERT
        # =================================================

        try:

            cursor.execute(
                """
                INSERT INTO market_price_events (
                    symbol,
                    name,
                    price,
                    volume,
                    event_time
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    event["symbol"],
                    event["name"],
                    event["price"],
                    event["volume"],
                    event["event_time"]
                )
            )

            conn.commit()

        except psycopg.Error as e:

            print(
                "DB 저장 오류:",
                e
            )

            print(
                "문제 Event:",
                event
            )

            # PostgreSQL Transaction이 오류 상태에 빠졌으므로
            # 해당 작업을 취소하고 다음 Event를 받을 수 있게 복구
            conn.rollback()

            continue




        print(
            "Inserted:",
            event["symbol"],
            event["event_time"],
            event["price"],
            "volume:",
            event["volume"]
        )


# =========================================================
# 8. 프로그램 종료 시 자원 정리
# =========================================================

finally:

    # Ctrl + C 등으로 종료되더라도
    # DB 및 Kafka 연결을 정상적으로 닫는다.
    cursor.close()

    conn.close()

    consumer.close()