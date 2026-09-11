import requests
import os
import json

APP_KEY = "PSBPk3BdffQreQpNSMEF4Ee18kGP2F2PGjrs"
APP_SECRET = "bwpJHmGBiC6ainuNwrpXvU9PPRHCUN5Q2oHkFn1TSN28+PJsafcZIfQlWVgQfN5nOzCdbVDf8eXHrRFu8hBaNT/aNiUmHJwGCAAnL6UwwS07EFdQwKtokhhyhnrHjoZIn+qUOO4skoZjSGSZKSS/rtpDCbeq5BGWFr+88DGsemGSFnFekIQ="

url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"

headers = {
    "content-type": "application/json",
}

body = {
    "grant_type": "client_credentials",
    "appkey": APP_KEY,
    "secretkey": APP_SECRET
}

response = requests.post(
    url,
    headers=headers,
    data=json.dumps(body)
)

print(response.json())