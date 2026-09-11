from confluent_kafka import Producer
import json


producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


bad_event = {
    "symbol": "005930",
    "bar_time": "2026-09-10T20:00:00",
    "open_price": 259000,
    "high_price": 250000,
    "low_price": 258000,
    "close_price": 259500,
    "volume": -100
}


producer.produce(
    topic="market-minute-events",
    key="005930",
    value=json.dumps(bad_event)
)

producer.flush()

print("Bad event sent.")