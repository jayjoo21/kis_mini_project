from confluent_kafka import Producer
import json
import random #주식 가격 자동 생성(임시로)
import time
from datetime import datetime

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

try:
    while True: #항상 조건이 참이므로 무한반복
        #event 생성 -> 전송 -> 2초 대기 -> event 생성 -> 전송 -> 2초 대기 반복

        event = {
            "symbol": "005930",
            "name": "Samsung Electronics",
            "price": random.randint(70000, 75000), #rand=random, int=integer
            #70000~75000 사이의 랜덤한 정수 생성
            "volume": random.randint(1000000, 2000000), 
            "event_time": datetime.now().isoformat()
        }

        producer.produce(
            topic="market-price-events",
            key=event["symbol"],
            value=json.dumps(event)
        )

        producer.poll(0) # Trigger delivery callbacks

        print("Sent event:", event)

        time.sleep(2) #2초마다 새 event 발생

except KeyboardInterrupt:
    print("\nProducer stopped")

finally:
    producer.flush()
