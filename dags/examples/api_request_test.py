import requests
import json

nickname = 'vyushmanov'
cohort = '16'
api_token = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
sort_field = 'id'
sort_direction = 'asc'
limit = '50'
offset = '0'
headers = {
    "sort_field": sort_field,
    "sort_direction": sort_direction,
    "limit": limit,
    "offset": offset,
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}
api_endpoint = 'd5d04q7d963eapoepsqr.apigw.yandexcloud.net'
method_url = '/restaurants'

r = requests.get('https://' + api_endpoint + method_url, headers=headers)
print(r)