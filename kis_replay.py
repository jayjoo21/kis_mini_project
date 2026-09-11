"""
과거 분봉 데이터를 KIS REST API로 가져온 뒤
시간순으로 정렬해서 Kafka에 Replay하는 코드

전체 흐름

KIS REST API
    ↓
Access Token 발급
    ↓
삼성전자 특정 날짜 분봉 조회
    ↓
한 번에 최대 120건씩 과거 방향 반복 조회
    ↓
중복 제거
    ↓
시간순 정렬
    ↓
Kafka Producer
    ↓
market-minute-events Topic
"""


# =========================================================
# 1. 라이브러리
# =========================================================

import requests
import json
import time

from confluent_kafka import Producer


# =========================================================
# 2. 기본 설정
# =========================================================

APP_KEY = "PSBPk3BdffQreQpNSMEF4Ee18kGP2F2PGjrs"
APP_SECRET = "bwpJHmGBiC6ainuNwrpXvU9PPRHCUN5Q2oHkFn1TSN28+PJsafcZIfQlWVgQfN5nOzCdbVDf8eXHrRFu8hBaNT/aNiUmHJwGCAAnL6UwwS07EFdQwKtokhhyhnrHjoZIn+qUOO4skoZjSGSZKSS/rtpDCbeq5BGWFr+88DGsemGSFnFekIQ="

BASE_URL = "https://openapi.koreainvestment.com:9443"

STOCK_CODE = "005930"
TARGET_DATE = "20260910"



# =========================================================
# 3. KIS Access Token 발급
# =========================================================

def get_access_token():

    # KIS에서 Access Token을 발급해주는 API 주소
    #
    # Access Token:
    # APP_KEY / APP_SECRET으로 인증한 뒤 받는 임시 인증 토큰.
    # 이후 주가 API를 호출할 때
    #
    # Authorization: Bearer <access_token>
    #
    # 형태로 보내서 "인증된 요청"임을 증명한다.
    url = f"{BASE_URL}/oauth2/tokenP"


    # HTTP Header
    #
    # content-type:
    # "내가 서버에 보내는 Body 데이터는 JSON 형식입니다."
    # 라고 서버에게 알려주는 정보
    headers = {
        "content-type": "application/json"
    }


    # HTTP POST 요청의 Body
    #
    # grant_type = client_credentials
    #
    # 사용자 ID/PW를 로그인시키는 방식이 아니라
    # APP_KEY + APP_SECRET이라는
    # 애플리케이션 자체의 자격증명으로 인증하겠다는 의미
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }


    # requests.post()
    #
    # POST 방식으로 KIS 인증 서버에 요청
    #
    # json.dumps(body):
    # Python dict를 JSON 문자열로 변환
    #
    # Python:
    # {"appkey": "..."}
    #
    # ↓ json.dumps()
    #
    # JSON 문자열:
    # '{"appkey": "..."}'
    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(body)
    )


    # raise_for_status()
    #
    # HTTP 통신 자체가 실패했는지 검사한다.
    #
    # 예:
    # 200 → 정상
    # 400 → 잘못된 요청
    # 401 → 인증 실패
    # 403 → 권한 없음
    # 404 → 주소/자원 없음
    # 500 → 서버 오류
    #
    # 4xx 또는 5xx라면 여기서 Exception을 발생시켜
    # 잘못된 응답을 가지고 뒤 코드를 계속 실행하지 않게 한다.
    #
    # 중요:
    # 이것은 "HTTP 통신 성공 여부"를 검사하는 것이고,
    # 뒤에서 보는 KIS의 rt_cd와는 역할이 다르다.
    response.raise_for_status()


    # response.json()
    #
    # 서버가 돌려준 JSON 응답을
    # Python dict 형태로 변환한다.
    #
    # 예:
    #
    # {
    #     "access_token": "...",
    #     "token_type": "Bearer",
    #     ...
    # }
    #
    # 그중 access_token 값만 꺼내서 함수 밖으로 반환
    return response.json()["access_token"]



# =========================================================
# 4. 특정 시간을 기준으로 분봉 데이터 조회
# =========================================================

def get_minute_data(access_token, current_time):

    # 국내주식 "주식일별분봉조회" API 주소
    #
    # 한 번의 API 호출로 하루 전체를 받는 게 아니라
    # 특정 시간을 기준으로 일정 개수의 분봉을 가져온다.
    #
    # 그래서 아래 get_full_day_minute_data()에서
    # 이 함수를 여러 번 호출하게 된다.
    url = (
        f"{BASE_URL}"
        "/uapi/domestic-stock/v1/quotations/"
        "inquire-time-dailychartprice"
    )


    headers = {

        # JSON + UTF-8 문자 인코딩을 사용한다는 의미
        "content-type": "application/json; charset=utf-8",


        # Bearer:
        # 발급받은 Access Token을 가지고 있다는 것을
        # 인증 정보로 전달하는 방식
        #
        # 실제 요청 Header는 대략:
        #
        # Authorization: Bearer eyJ....
        #
        # 형태가 된다.
        "authorization": f"Bearer {access_token}",


        # KIS가 어떤 애플리케이션의 요청인지 확인하기 위해
        # API 요청 Header에도 APP_KEY / APP_SECRET을 요구한다.
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,


        # tr_id:
        # KIS가 "어떤 API 업무를 요청하는지" 구별하는 코드.
        #
        # FHKST03010230
        # = 주식일별분봉조회 API의 TR ID
        #
        # URL이 같거나 비슷한 API라도
        # KIS에서는 TR ID를 통해 요청 업무를 구분하기 때문에
        # 문서에서 지정한 값을 정확히 보내야 한다.
        "tr_id": "FHKST03010230",


        # custtype:
        # 고객 유형을 나타내는 KIS Header 값.
        # 개인 고객 요청에서는 "P"를 사용한다.
        "custtype": "P"
    }


    # params:
    #
    # GET API에 넘길 조회 조건.
    #
    # 예:
    # 어떤 시장?
    # 어떤 종목?
    # 어떤 날짜?
    # 어떤 시간을 기준으로?
    #
    # 같은 정보를 담는다.
    #
    # FID_로 시작하는 이름들은
    # 우리가 임의로 만든 변수명이 아니라
    # KIS API 명세에서 정한 "요청 필드 이름"이다.
    #
    # 따라서 stock_code 같은 임의 이름으로 바꾸면 안 된다.
    params = {

        # FID_COND_MRKT_DIV_CODE
        #
        # COND = Condition
        # MRKT = Market
        # DIV  = Division
        #
        # 시장 구분 조건.
        #
        # 여기서 "J"는 주식 시장 조회에 사용하는 값.
        "FID_COND_MRKT_DIV_CODE": "J",


        # FID_INPUT_ISCD
        #
        # 조회할 종목코드를 전달하는 KIS 요청 필드.
        "FID_INPUT_ISCD": STOCK_CODE,


        # FID_INPUT_HOUR_1
        #
        # 조회 기준 시간.
        #
        # HHMMSS 형식:
        #
        # 153000
        # → 15:30:00
        #
        # 처음에는 15:30을 넣고,
        # 조회 결과 중 가장 과거 시간을 찾아
        # 다음 요청에서 다시 이 값으로 사용한다.
        #
        # 중요:
        #
        # current_time     → 변수 안의 값 전송
        # "current_time"   → current_time이라는 문자 자체 전송
        #
        # 따라서 따옴표를 붙이면 안 된다.
        "FID_INPUT_HOUR_1": current_time,


        # FID_INPUT_DATE_1
        #
        # 조회할 영업일.
        #
        # YYYYMMDD 형식 사용
        "FID_INPUT_DATE_1": TARGET_DATE,


        # FID_PW_DATA_INCU_YN
        #
        # KIS 명세상 "과거 데이터 포함 여부".
        #
        # Y = 포함
        #
        # 현재 날짜의 실시간 근처 데이터가 아니라
        # TARGET_DATE처럼 과거 날짜 데이터를 조회하므로 Y 사용
        "FID_PW_DATA_INCU_YN": "Y",


        # FID_FAKE_TICK_INCU_YN
        #
        # KIS 명세상 "허봉 포함 여부" 관련 옵션.
        #
        # 지금 실습에서는 허봉을 별도로 포함하도록 지정하지 않고
        # 기본값인 빈 문자열을 사용한다.
        #
        # 이 값 역시 우리가 만든 옵션이 아니라
        # KIS API에서 요구하는 요청 필드다.
        "FID_FAKE_TICK_INCU_YN": ""
    }


    # requests.get()
    #
    # GET은 서버에 데이터를 "조회"할 때 주로 사용한다.
    #
    # headers:
    # 인증정보 / TR ID 등
    #
    # params:
    # 종목 / 날짜 / 시간 등의 조회조건
    #
    # requests가 params를 URL Query Parameter 형태로
    # 알아서 변환해서 전송한다.
    response = requests.get(
        url,
        headers=headers,
        params=params
    )


    # HTTP 수준 오류 검사
    #
    # 예를 들어 서버가 401, 404, 500 등을 반환하면
    # 여기서 바로 Exception 발생
    response.raise_for_status()


    # KIS가 반환한 JSON을 Python dict로 변환
    result = response.json()


    # rt_cd:
    #
    # KIS 응답에서 "업무 처리 성공/실패"를 나타내는 코드.
    #
    # "0" → 정상 처리
    # "0" 이외 → API 업무 처리 실패
    #
    #
    # response.raise_for_status()와 차이
    # ---------------------------------
    #
    # response.raise_for_status()
    # → HTTP 통신 자체가 성공했는지 검사
    #
    # result["rt_cd"]
    # → KIS가 요청한 업무를 정상적으로 처리했는지 검사
    #
    #
    # 예를 들어 HTTP는 200 OK인데
    # 종목코드나 요청조건이 잘못되면
    # KIS 응답 내부 rt_cd는 실패값일 수 있다.
    if result["rt_cd"] != "0":

        # 실패 시 result 전체를 출력하면 보통
        #
        # rt_cd  → 성공/실패 여부
        # msg_cd → KIS의 세부 응답/오류 코드
        # msg1   → 사람이 읽을 수 있는 오류 메시지
        #
        # 등을 확인할 수 있어서 디버깅하기 좋다.
        print("API 오류:", result)

        return []


    # output2:
    #
    # KIS 주식일별분봉조회 응답에서
    # 실제 "여러 개의 분봉 행"이 들어있는 부분.
    #
    # 개념적으로:
    #
    # output2 = [
    #     {
    #         "stck_cntg_hour": "153000",
    #         "stck_oprc": "...",
    #         ...
    #     },
    #     {
    #         "stck_cntg_hour": "152900",
    #         ...
    #     }
    # ]
    #
    # 이 API에서 우리가 실제로 필요한 분봉 데이터가
    # output2에 들어 있으므로 이것만 반환한다.
    return result["output2"]



# =========================================================
# 5. 하루 전체 분봉 데이터 수집
# =========================================================

def get_full_day_minute_data(access_token):

    # 여러 API 요청에서 받은 분봉들을
    # 전부 하나로 모아둘 리스트
    all_rows = []


    # 첫 번째 조회는 15:30부터 시작
    #
    # API가 한 번에 전체 하루 데이터를 주지 않기 때문에
    # 15:30 → 과거 방향으로 계속 내려간다.
    current_time = "153000"


    # break 조건을 만날 때까지 계속 반복
    while True:


        # 현재 시간을 기준으로 분봉 한 묶음 조회
        #
        # 첫 반복:
        # current_time = 153000
        #
        # 다음 반복:
        # current_time = 이전 결과에서 발견한 가장 이른 시간
        rows = get_minute_data(
            access_token,
            current_time
        )


        # API 결과가 빈 리스트라면
        # 더 이상 처리할 데이터가 없다는 의미이므로 종료
        if not rows:
            break


        # 혹시 응답 안에 시간 필드가 비어 있는 행이 있을 경우를 대비해
        # stck_cntg_hour 값이 있는 데이터만 골라낸다.
        #
        # row.get("stck_cntg_hour"):
        #
        # row["stck_cntg_hour"]와 달리
        # 해당 Key가 없더라도 KeyError를 내지 않고 None을 반환한다.
        valid_rows = [
            row
            for row in rows
            if row.get("stck_cntg_hour")
        ]


        if not valid_rows:
            break


        # extend:
        #
        # 이번 API 호출에서 받은 여러 행을
        # all_rows에 펼쳐서 추가한다.
        #
        # append(valid_rows)를 하면
        #
        # [
        #     [120개 데이터],
        #     [120개 데이터]
        # ]
        #
        # 처럼 리스트 안에 리스트가 생기지만
        #
        # extend(valid_rows)는
        #
        # [
        #     row1,
        #     row2,
        #     row3,
        #     ...
        # ]
        #
        # 형태로 계속 이어 붙인다.
        all_rows.extend(valid_rows)


        # 이번 API 응답에서 시간만 뽑아낸다.
        times = [
            row["stck_cntg_hour"]
            for row in valid_rows
        ]


        # 이번 조회에서 가장 이른 시간 찾기
        #
        # 예:
        #
        # 153000
        # 152900
        # ...
        # 133100
        #
        # min() 결과
        # → 133100
        #
        # 시간 문자열이 모두 HHMMSS 6자리 형식이므로
        # 문자열 비교를 해도 시간 순서와 동일하게 동작한다.
        min_time = min(times)


        print(
            "조회 기준:",
            current_time,
            "| 가장 이른 시간:",
            min_time,
            "| 조회 건수:",
            len(valid_rows)
        )


        # 정규장 시작 시간인 09:00까지 내려왔다면
        # 하루치 조회가 끝났다고 보고 반복 종료
        if min_time <= "090000":
            break


        # API 한 번에 최대 120건을 받는 구조에서
        # 120건보다 적게 왔다면
        # 더 과거에 가져올 데이터가 충분히 남지 않았다고 보고 종료
        #
        # API 상황에 따라 종료 판단용 안전장치 역할도 한다.
        if len(valid_rows) < 120:
            break


        # 혹시 API가 계속 동일한 마지막 시간만 반환하면
        #
        # 13:31 조회
        # → 가장 이른 시간 13:31
        # → 다시 13:31 조회
        # → 또 13:31
        #
        # 처럼 무한 반복할 수 있다.
        #
        # 그래서 시간이 실제로 뒤로 이동하지 않으면 종료한다.
        if min_time == current_time:

            print(
                "조회 시간이 더 이상 과거로 이동하지 않아 종료합니다."
            )

            break


        # 다음 반복에서는
        # 이번에 발견한 가장 과거 시간을 기준으로 다시 조회
        #
        # 예:
        #
        # 153000
        # ↓
        # 133100
        # ↓
        # 113200
        # ↓
        # ...
        current_time = min_time


        # API를 너무 빠르게 연속 호출하면
        # Rate Limit(호출 횟수 제한)에 걸릴 수 있기 때문에
        # 요청 사이에 잠시 대기한다.
        #
        # 0.2초는 실습용 간격이며
        # 실제 운영에서는 API의 호출 제한 정책에 맞춰 조정해야 한다.
        time.sleep(0.2)


    # 여러 번의 API 호출에서 누적한 전체 데이터를 반환
    return all_rows



# =========================================================
# 6. Token 발급 후 하루 전체 데이터 수집
# =========================================================

# 실제로 Token 발급 API 호출
access_token = get_access_token()


# 위에서 만든 반복 조회 함수를 실행해
# 해당 날짜의 하루 분봉을 모두 가져온다.
rows = get_full_day_minute_data(
    access_token
)


print()
print(
    "중복 제거 전 전체 건수:",
    len(rows)
)



# =========================================================
# 7. API 호출 구간 사이의 중복 제거
# =========================================================

# API를 여러 번 이어서 조회하면
# 호출 경계에 같은 시각의 분봉이 중복될 수 있다.
#
# 예:
#
# 1차 요청 마지막:
# 13:31
#
# 2차 요청 첫 부분:
# 13:31
#
# 따라서 Kafka로 보내기 전에
# Python 단계에서도 한 번 중복 제거한다.
unique_rows = {}


for row in rows:


    # 한 개 분봉의 고유 기준
    #
    # 종목 + 날짜 + 시간
    #
    # 예:
    #
    # ("005930", "20260910", "133100")
    #
    # 이 세 값이 같다면
    # 동일한 종목의 동일 시점 분봉이라고 판단한다.
    #
    # 이것은 우리가 계속 이야기한 Grain과 연결된다.
    #
    # Grain:
    # 현재 데이터 한 행 = 한 종목의 한 시점 1분봉
    key = (
        STOCK_CODE,
        TARGET_DATE,
        row["stck_cntg_hour"]
    )


    # Dictionary는 동일한 Key를 다시 넣으면
    # 기존 Value가 새 Value로 덮어써진다.
    #
    # 따라서 동일한 종목 + 날짜 + 시간 데이터가
    # 여러 번 있어도 최종적으로 하나만 남는다.
    unique_rows[key] = row


# Dictionary에서 중복 제거된 Value들만 꺼내
# 다시 일반 List 형태로 변환
rows = list(
    unique_rows.values()
)


print(
    "중복 제거 후 전체 건수:",
    len(rows)
)



# =========================================================
# 8. 시간 오름차순 정렬
# =========================================================

# API는 과거 방향으로 조회했기 때문에
#
# 15:30
# 15:29
# 15:28
# ...
#
# 식으로 데이터가 섞여 있을 수 있다.
#
# Kafka Replay는 실제 시간이 흐르듯
#
# 09:00
# 09:01
# 09:02
# ...
#
# 순으로 보내기 위해 정렬한다.
rows = sorted(
    rows,

    # lambda x:
    # 각 row에서 stck_cntg_hour 값을 꺼내
    # 그 값을 정렬 기준으로 사용
    key=lambda x: x["stck_cntg_hour"]
)



# 데이터가 실제로 존재할 경우에만
# 첫 시간 / 마지막 시간을 확인
#
# 하루 전체 조회가 제대로 됐는지 빠르게 점검하는 용도
if rows:

    print(
        "최초 시간:",
        rows[0]["stck_cntg_hour"]
    )

    print(
        "마지막 시간:",
        rows[-1]["stck_cntg_hour"]
    )



# =========================================================
# 9. Kafka Producer 생성
# =========================================================

producer = Producer({

    # Kafka Broker 접속 주소
    #
    # 현재 Docker Kafka의 9092 포트를
    # Windows localhost:9092로 연결해 두었기 때문에
    # Python에서는 이 주소로 Kafka에 접속한다.
    "bootstrap.servers": "localhost:9092"
})



# =========================================================
# 10. KIS 원본 데이터를 우리 Event Schema로 변환 후 Replay
# =========================================================

for row in rows:


    # KIS의 필드명을 그대로 Kafka에 보내지 않고
    # 우리가 정의한 Event Schema로 변환한다.
    #
    # KIS:
    #
    # stck_oprc
    # stck_hgpr
    # stck_lwpr
    # stck_prpr
    # cntg_vol
    #
    # ↓
    #
    # 우리 Kafka Event:
    #
    # open_price
    # high_price
    # low_price
    # close_price
    # volume
    #
    # 이렇게 외부 API의 구조와
    # 내부 파이프라인의 구조를 분리해두면
    # 나중에 데이터 공급처가 바뀌어도
    # Consumer와 DB 구조를 덜 변경해도 된다.
    event = {

        "symbol": STOCK_CODE,


        # KIS 날짜:
        # 20260910
        #
        # KIS 시간:
        # 133100
        #
        # ↓
        #
        # 2026-09-10T13:31:00
        #
        # 형태로 변환한다.
        #
        # ISO 8601과 유사한 날짜/시간 표현으로 만들어두면
        # JSON, Python datetime, PostgreSQL 등에서
        # 다루기 편해진다.
        "bar_time": (
            f'{TARGET_DATE[:4]}-'
            f'{TARGET_DATE[4:6]}-'
            f'{TARGET_DATE[6:8]}T'
            f'{row["stck_cntg_hour"][:2]}:'
            f'{row["stck_cntg_hour"][2:4]}:'
            f'{row["stck_cntg_hour"][4:6]}'
            f'+09:00'
        ),


        # KIS API가 숫자값을 문자열 형태로 반환하는 경우가 있으므로
        # DB에 숫자로 저장하고 계산할 수 있게 int로 변환
        "open_price": int(
            row["stck_oprc"]
        ),

        "high_price": int(
            row["stck_hgpr"]
        ),

        "low_price": int(
            row["stck_lwpr"]
        ),

        "close_price": int(
            row["stck_prpr"]
        ),

        "volume": int(
            row["cntg_vol"]
        )
    }



    # Kafka에 Event 발행
    #
    # topic:
    # 분봉 Event 전용 Topic
    #
    # key:
    # 현재는 종목코드를 Kafka Message Key로 사용
    #
    # 동일한 Key는 일반적으로 같은 Partition으로 가므로
    # 이후 Partition을 여러 개로 확장했을 때
    # 동일 종목 데이터 순서를 유지하는 설계에 활용할 수 있다.
    #
    # value:
    # Python dict는 그대로 Kafka에 보낼 수 없으므로
    # json.dumps()로 JSON 문자열로 직렬화해서 전송한다.
    #
    # Serialization(직렬화):
    # Python 객체를 전송/저장 가능한 형태로 변환하는 것
    producer.produce(
        topic="market-minute-events-v2",
        key=STOCK_CODE,
        value=json.dumps(event)
    )


    # Producer는 내부적으로 비동기 방식으로 메시지를 보낼 수 있다.
    #
    # poll(0)은 기다리지는 않으면서
    # 내부 Delivery Event 등의 처리를 수행할 기회를 준다.
    producer.poll(0)


    print(
        "Replay:",
        event
    )


    # 과거 데이터 수백 건을 한 순간에 모두 보내는 대신
    # 0.5초 간격으로 흘려보내
    # Streaming 상황처럼 관찰하기 쉽게 만든다.
    #
    # 즉 실제 거래 발생 간격을 재현하는 것은 아니고
    # 학습용 Replay 속도다.
    time.sleep(0.5)



# =========================================================
# 11. Producer 내부에 남은 Event까지 모두 Kafka로 전송
# =========================================================

# Kafka Producer는 produce()를 호출했다고 해서
# 그 순간 모든 메시지가 서버 전송 완료된다고 보장되지 않는다.
#
# 내부 Buffer에 아직 남아 있을 수 있기 때문에
# 프로그램을 끝내기 전에 flush()를 호출한다.
#
# 즉:
#
# "아직 전송 안 끝난 메시지가 있으면
#  다 보내고 나서 프로그램 끝내."
producer.flush()


print()
print("Replay 완료")
