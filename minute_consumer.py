from confluent_kafka import Consumer
import json
import psycopg
from datetime import datetime


# =========================================================
# 1. Data Quality 검증 함수
# =========================================================

def validate_event(event):

    errors = [] #오류 담을 빈 리스트

    required_fields = [ #반드시 있어야 하는 데이터 목록
        "symbol",
        "bar_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume"
    ]

    # 필수 컬럼 확인
    for field in required_fields:
        if field not in event:
            errors.append(f"missing field: {field}")

    # 필수 컬럼이 빠졌으면 이후 검사를 할 수 없으므로 종료
    if errors:
        return errors

    # 가격이 양수인지 확인
    price_fields = [
        "open_price",
        "high_price",
        "low_price",
        "close_price"
    ]

    for field in price_fields:
        if event[field] <= 0:
            errors.append(
                f"{field} must be positive"
            )

    # 거래량 검사
    if event["volume"] < 0:
        errors.append(
            "volume cannot be negative"
        )

    # OHLC 관계 검사
    if event["high_price"] < max(
        event["open_price"],
        event["close_price"],
        event["low_price"]
    ):
        errors.append(
            "high_price is invalid"
        )

    if event["low_price"] > min(
        event["open_price"],
        event["close_price"],
        event["high_price"]
    ):
        errors.append(
            "low_price is invalid"
        )

    # 시간 형식 검사
    try:
        datetime.fromisoformat(
            event["bar_time"]
        )

    except (ValueError, TypeError):
        errors.append(
            "bar_time format is invalid"
        )

    return errors


# =========================================================
# 2. Kafka Consumer 설정
# =========================================================

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "market-minute-db-group-v2",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([
    "market-minute-events"
])


# =========================================================
# 3. PostgreSQL(db) 연결
# =========================================================

conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="marketdb",
    user="kafkauser",
    password="kafkapass"
)

cursor = conn.cursor()

print("waiting for minute-bar events...")


# =========================================================
# 4. Kafka 메시지 계속 수신
# =========================================================

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        raw_value = msg.value().decode("utf-8")


        # =================================================
        # 5. JSON Parsing
        # =================================================

        try:
            event = json.loads(raw_value)  #json -> python 객체

        except json.JSONDecodeError:
            print("JSON 오류:", raw_value)
            continue


        # =================================================
        # 6. Data Quality 검사
        # =================================================

        errors = validate_event(event)

        if errors:
            print("DQ FAIL:", event)
            print("Reasons:", errors)
            continue


        # =================================================
        # 7. PostgreSQL INSERT
        # =================================================

        cursor.execute(
            """
            INSERT INTO market_minute_bars ( 
                symbol,
                bar_time,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, bar_time)
            DO NOTHING
            """,
            (
                event["symbol"],
                event["bar_time"],
                event["open_price"],
                event["high_price"],
                event["low_price"],
                event["close_price"],
                event["volume"]
            )
        )

        conn.commit()


        # =================================================
        # 8. INSERT 결과 확인
        # =================================================

        if cursor.rowcount == 1: #rowcount: sql 때문에 영향을 받은 행 수
            print(
                "Inserted:",
                event["symbol"],
                event["bar_time"],
                event["close_price"]
            )

        else:
            print(
                "Duplicate skipped:",
                event["symbol"],
                event["bar_time"]
            )


# =========================================================
# 9. 프로그램 종료 시 연결 정리
# =========================================================

finally:
    cursor.close()
    conn.close()
    consumer.close()