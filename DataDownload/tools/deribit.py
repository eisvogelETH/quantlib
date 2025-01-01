import requests
import json

Deribit_URL = 'https://www.deribit.com/api/v2/'

def get_book_summary_by_currency(currency, kind='option'):
    return {
        'method' : 'public/get_book_summary_by_currency',
        'kwargs' : {
            'currency' : currency,
            'kind'     : kind,
        }
    }

def web_request(function):
    msg = {
        'method'  : function['method'],
        'params'  : function['kwargs'],
        'jsonrpc' : '2.0',
        'id'      : 2,
        }
    resp = requests.post(
        url=Deribit_URL, 
        data=json.dumps(msg), 
        headers={'content-type': 'application/json'})
    return resp
#

def get_option_order_book(asset='BTC'):
    # Download BTC option data from Deribit and convert to dataframe
    resp  = web_request(function=get_book_summary_by_currency(currency=asset))
    print('Response:', resp.status_code)
    result = resp.json()['result'] if resp.status_code == 200 else None
    return result
