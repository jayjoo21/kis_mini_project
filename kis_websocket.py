import os
import json
import asyncio
import requests
import websockets

from dotenv import load_dotenv
from confluent_kafka import Producer
from websockets.exceptions import ConnectionClosed


# =========================================================
# 환경변수 / 기본 설정
# =========================================================

load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")

if not APP_KEY or not APP_SECRET:
    raise ValueError(
        ".env에서 KIS_APP_KEY 또는 KIS_APP_SECRET을 찾을 수 없습니다."
    )


BASE_URL = "https://openapi.koreainvestment.com:9443"
WS_URL = "ws://ops.koreainvestment.com:21000"

STOCK_CODE = "005930"
STOCK_NAME = "삼성전자"

# 국내주식 실시간 체결
TR_ID = "H0STCNT0"

# 현재 KIS H0STCNT0 체결 1건의 필드 수
FIELD_COUNT = 47


# =========================================================
# Kafka Producer
# =========================================================

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


# =========================================================
# WebSocket Approval Key 발급
# =========================================================

def get_approval_key():

    url = f"{BASE_URL}/oauth2/Approval"

    headers = {
        "content-type": "application/json"
    }

    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET
    }

    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(body),
        timeout=10
    )

    response.raise_for_status()

    result = response.json()

    if "approval_key" not in result:
        raise ValueError(
            f"Approval Key 발급 실패: {result}"
        )

    return result["approval_key"]


# =========================================================
# KIS 체결 데이터 → 우리 Event 형태로 변환
# =========================================================

def parse_trade_values(values):

    symbol = values[0]
    trade_time = values[1]
    price = int(values[2])
    trade_volume = int(values[12])
    business_date = values[33]

    event_time = (
        f"{business_date[:4]}-"
        f"{business_date[4:6]}-"
        f"{business_date[6:8]}T"
        f"{trade_time[:2]}:"
        f"{trade_time[2:4]}:"
        f"{trade_time[4:6]}"
        f"+09:00"
    )

    return {
        "symbol": symbol,
        "name": STOCK_NAME,
        "price": price,
        "volume": trade_volume,
        "event_time": event_time
    }


# =========================================================
# Kafka 전송
# =========================================================

def send_to_kafka(event):

    producer.produce(
        topic="market-tick-events-v2",
        key=event["symbol"],
        value=json.dumps(
            event,
            ensure_ascii=False
        )
    )

    producer.poll(0)


# =========================================================
# 실시간 WebSocket
# =========================================================

async def main():

    while True:

        try:

            approval_key = get_approval_key()

            print("Approval Key 발급 완료")
            print("WebSocket 연결 시도...")


            subscribe_message = {
                "header": {
                    "approval_key": approval_key,
                    "custtype": "P",
                    "tr_type": "1",
                    "content-type": "utf-8"
                },
                "body": {
                    "input": {
                        "tr_id": TR_ID,
                        "tr_key": STOCK_CODE
                    }
                }
            }


            async with websockets.connect(
                WS_URL,
                ping_interval=None
            ) as websocket:

                print("WebSocket 연결 성공")

                # 재연결할 때마다 다시 구독해야 한다.
                await websocket.send(
                    json.dumps(subscribe_message)
                )

                print(
                    f"{STOCK_CODE} 실시간 체결 구독 요청 완료"
                )


                while True:

                    data = await websocket.recv()


                    # =================================================
                    # 실시간 체결 데이터
                    # =================================================

                    if (
                        isinstance(data, str)
                        and data.startswith("0|")
                    ):

                        parts = data.split("|")

                        if len(parts) < 4:
                            print(
                                "잘못된 실시간 메시지:",
                                data
                            )
                            continue


                        tr_id = parts[1]

                        if tr_id != TR_ID:
                            continue


                        try:
                            data_count = int(parts[2])

                        except ValueError:
                            print(
                                "잘못된 data_count:",
                                parts[2]
                            )
                            continue


                        values = parts[3].split("^")

                        expected_count = (
                            data_count * FIELD_COUNT
                        )


                        if len(values) != expected_count:

                            print(
                                "필드 개수 불일치:",
                                "data_count =",
                                data_count,
                                "실제 =",
                                len(values),
                                "예상 =",
                                expected_count
                            )

                            continue


                        for i in range(data_count):

                            start = i * FIELD_COUNT
                            end = start + FIELD_COUNT

                            one_trade = values[start:end]


                            if len(one_trade) != FIELD_COUNT:

                                print(
                                    "체결 필드 개수 오류:",
                                    len(one_trade)
                                )

                                continue


                            try:

                                event = parse_trade_values(
                                    one_trade
                                )

                            except (
                                ValueError,
                                IndexError
                            ) as e:

                                print(
                                    "체결 파싱 오류:",
                                    e
                                )

                                continue


                            # 체결 1건당 Kafka 전송 1회
                            send_to_kafka(event)


                            print(
                                "REALTIME:",
                                event["symbol"],
                                event["event_time"],
                                event["price"],
                                "volume:",
                                event["volume"]
                            )


                    # =================================================
                    # 구독 응답 / PINGPONG
                    # =================================================

                    else:

                        try:
                            json_data = json.loads(data)

                        except (
                            json.JSONDecodeError,
                            TypeError
                        ):

                            print(
                                "알 수 없는 메시지:",
                                data
                            )

                            continue


                        tr_id = (
                            json_data
                            .get("header", {})
                            .get("tr_id")
                        )


                        if tr_id == "PINGPONG":

                            await websocket.pong(data)

                            continue


                        body = json_data.get(
                            "body",
                            {}
                        )

                        rt_cd = body.get("rt_cd")
                        msg1 = body.get("msg1")


                        if rt_cd == "0":

                            print(
                                "SUBSCRIBE SUCCESS:",
                                msg1
                            )

                        else:

                            print(
                                "WebSocket 응답:",
                                json_data
                            )


        # 연결이 끊겨도 프로그램 자체는 종료하지 않는다.
        except ConnectionClosed as e:

            print(
                "WebSocket 연결 종료:",
                e
            )

            print(
                "5초 후 재연결 시도"
            )

            await asyncio.sleep(5)


        except (
            OSError,
            requests.RequestException
        ) as e:

            print(
                "네트워크/API 오류:",
                e
            )

            print(
                "5초 후 재연결 시도"
            )

            await asyncio.sleep(5)


# =========================================================
# 실행
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print()
        print("WebSocket 종료")

    finally:

        producer.flush()