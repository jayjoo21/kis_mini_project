from confluent_kafka import Consumer
import json
import psycopg

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "market-db-group-v1",
    "auto.offset.reset": "latest"
})

consumer.subscribe(['market-price-events'])

#conn: connection(python - postgresql사이의 연결)
conn = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="marketdb",
    user="kafkauser",
    password="kafkapass"
)

#connection: db와 연결
#cursor: db에 sql 명령을 보내는 도구/ 그 연결을 통해 sql 실행

cursor = conn.cursor()

print("waiting for kafka messages...")

try:
    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("kafka error:", msg.error())
            continue

        raw_value = msg.value().decode("utf-8")

        try:
            event = json.loads(raw_value)

        except json.JSONDecodeError:
            print("Failed to decode JSON:", raw_value)
            continue

        cursor.execute(
            """
            INSERT INTO market_price_events
            (symbol, name, price, volume, event_time)
            VALUES (%s, %s, %s, %s, %s)
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

        print(
            "saved:",
            event["symbol"],
            event["name"],
            event["event_time"]
        )

finally:
    cursor.close()
    conn.close()
    consumer.close()