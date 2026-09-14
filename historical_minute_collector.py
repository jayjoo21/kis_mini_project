import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")

BASE_URL = "https://openapi.koreainvestment.com:9443"

STOCK_CODE = "005930"
STOCK_NAME = "삼성전자"

# 우선 5일만 테스트
START_DATE = "20260907"
END_DATE = "20260911"


# =========================================================
# Access Token
# =========================================================

def get_access_token():

    response = requests.post(
        f"{BASE_URL}/oauth2/tokenP",
        headers={
            "content-type": "application/json"
        },
        json={
            "grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET
        },
        timeout=10
    )

    response.raise_for_status()

    result = response.json()

    return result["access_token"]


# =========================================================
# 특정 날짜 + 특정 시각 기준 분봉 조회
# =========================================================

def get_minute_chunk(
    access_token,
    target_date,
    current_time
):

    headers = {
        "content-type": "application/json",
        "authorization": f"Bearer {access_token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST03010230",
        "custtype": "P"
    }

    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": STOCK_CODE,
        "FID_INPUT_HOUR_1": current_time,
        "FID_INPUT_DATE_1": target_date,
        "FID_PW_DATA_INCU_YN": "Y",
        "FID_FAKE_TICK_INCU_YN": ""
    }

    response = requests.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice",
        headers=headers,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    result = response.json()

    if result["rt_cd"] != "0":

        raise RuntimeError(
            f"KIS API 오류: {result.get('msg_cd')} "
            f"{result.get('msg1')}"
        )

    return result.get("output2", [])


# =========================================================
# 하루 전체 분봉
# =========================================================

def get_one_day(
    access_token,
    target_date
):

    current_time = "153000"

    all_rows = []


    while True:

        rows = get_minute_chunk(
            access_token,
            target_date,
            current_time
        )


        # 해당 날짜의 데이터만 사용
        valid_rows = [
            row
            for row in rows
            if row.get("stck_bsop_date") == target_date
        ]


        if not valid_rows:
            break


        all_rows.extend(
            valid_rows
        )


        times = [
            row["stck_cntg_hour"]
            for row in valid_rows
        ]

        oldest_time = min(times)


        if oldest_time <= "090000":
            break


        # 같은 데이터가 반복 조회되지 않도록
        # 가장 오래된 시각보다 1분 전으로 이동
        previous_time = (
            datetime.strptime(
                oldest_time,
                "%H%M%S"
            )
            - timedelta(minutes=1)
        )

        current_time = previous_time.strftime(
            "%H%M%S"
        )


        time.sleep(0.25)


    # 중복 제거
    unique_rows = {}

    for row in all_rows:

        key = (
            target_date,
            row["stck_cntg_hour"]
        )

        unique_rows[key] = row


    rows = list(
        unique_rows.values()
    )


    # 시간순 정렬
    rows = sorted(
        rows,
        key=lambda x: x["stck_cntg_hour"]
    )


    return rows


# =========================================================
# 모델링용 형태로 변환
# =========================================================

def convert_rows(
    rows,
    target_date
):

    result = []


    for row in rows:

        trade_time = row["stck_cntg_hour"]

        bar_time = (
            f"{target_date[:4]}-"
            f"{target_date[4:6]}-"
            f"{target_date[6:8]} "
            f"{trade_time[:2]}:"
            f"{trade_time[2:4]}:"
            f"{trade_time[4:6]}"
        )


        result.append({
            "datetime": bar_time,
            "symbol": STOCK_CODE,
            "name": STOCK_NAME,
            "open": int(row["stck_oprc"]),
            "high": int(row["stck_hgpr"]),
            "low": int(row["stck_lwpr"]),
            "close": int(row["stck_prpr"]),
            "volume": int(row["cntg_vol"])
        })


    return result


# =========================================================
# 여러 날짜 수집
# =========================================================

def main():

    access_token = get_access_token()

    print("Access Token 발급 완료")


    # 평일 날짜 생성
    dates = pd.date_range(
        START_DATE,
        END_DATE,
        freq="B"
    )


    all_data = []


    for date in dates:

        target_date = date.strftime(
            "%Y%m%d"
        )


        print(
            f"{target_date} 수집 시작"
        )


        rows = get_one_day(
            access_token,
            target_date
        )


        print(
            f"{target_date}: {len(rows)}개 분봉"
        )


        if not rows:
            continue


        converted = convert_rows(
            rows,
            target_date
        )


        all_data.extend(
            converted
        )


    df = pd.DataFrame(
        all_data
    )


    if df.empty:

        print(
            "수집된 데이터가 없습니다."
        )

        return


    df["datetime"] = pd.to_datetime(
        df["datetime"]
    )


    df = (
        df
        .drop_duplicates(
            subset=[
                "symbol",
                "datetime"
            ]
        )
        .sort_values(
            "datetime"
        )
        .reset_index(
            drop=True
        )
    )


    print()
    print("=== 수집 결과 ===")
    print(df.head())
    print()
    print(df.tail())
    print()
    print("행 개수:", len(df))
    print()
    print(df.dtypes)


    os.makedirs(
        "data",
        exist_ok=True
    )


    output_path = (
        "data/"
        "samsung_005930_1m_raw.csv"
    )


    df.to_csv(
        output_path,
        index=False,
        encoding="utf-8-sig"
    )


    print()
    print(
        "저장 완료:",
        output_path
    )


if __name__ == "__main__":
    main()