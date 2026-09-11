#한국투자증권 서버와 WebSocket 연결을 계속 유지하면서,
# 삼성전자 실시간 체결 데이터를 계속 받아오는 코드

import asyncio
#async: asynchronous
#io: input/output
#비동기 입출력

import json
import websockets

APPROVAL_KEY = "9c6615ff-b036-4f0c-861e-bbc97be6bc31"

WS_URL = "ws://ops.koreainvestment.com:21000"
#ws:websocket, ops.koreainvestment.com:21000=한국투자증권 서버 주소

#어떤 실시간 데이터를 받고 싶은지 서버에 알려주는 구독 요청서
subscribe_message = {
    "header": { #데이터의 부가정보, 누가, 어떤 형식으로 요청하는가
        "approval_key": APPROVAL_KEY,
        "custtype": "P", #customer type: p(personal)
        "tr_type": "1", #tr: transaction type(거래 타입), 1: 실시간 체결 데이터
        "content-type": "utf-8"
    },
    "body": { #실제로 뭘 요청하는가
        "input": {
            "tr_id": "H0STCNT0", #transaction id: 요청 종류 식별 id
            #H0STCNT0: 주식 실시간 체결 데이터
            "tr_key": "005930"} #어떤 종목?
    }
}

#이 함수 안에서는 시간이 걸리는 네트워크 작업을 비동기 방식으로 처리하겠음
async def main():

    async with websockets.connect( #이 주소로 WebSocket 연결 만들어줘
        WS_URL,
        ping_interval=None, #자동 ping 기능 끄기
    ) as websocket: #연결된 통신 객체 -> 변수이름으로 사용

        await websocket.send(
            json.dumps(subscribe_message)
        )

        print("삼성전자 실시간 체결 구독 요청 완료")

        while True:

            #전송 작업이 완료될 때까지 기다려
            data = await websocket.recv()
            #recv: receive, 서버가 보내주는 데이터를 기다려서 받음
            
            print("RAW:", repr(data))

            if isinstance(data, str) and data.startswith("0|HOSTCNT0|"):

                parts = data.split("|")

                raw_data = parts[3]

                values = raw_data.split("^")

                #parsing: 문자열을 의미 있는 단위로 나누는 작업
                symbol = values[0]
                trade_time = values[1]
                price = values[2]
                trade_volume = values[12]
                accmulated_volume = values[13]

                print("---------------------------")
                print("종목코드:", symbol)
                print("체결시간:", trade_time)
                print("체결가격:", price)
                print("체결수량:", trade_volume)
                print("누적거래량:", accmulated_volume)

            else:
                print("기타 message:", data)

asyncio.run(main())

