import json
import requests
import pandas as pd
from googleapiclient.discovery import build
from datetime import date, timedelta
from google.oauth2 import service_account
#from modules.sheets import *


def calculate_start_and_end_date(days_ago):
    """calculate min and max date using days"""
    max_date = (date.today()).strftime("%Y-%m-%d")
    min_date = (date.today() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    return min_date, max_date

def get_data_from_api( ti, days_ago=365, date_received_min='', date_received_max='' ):
    """this will get the required data from api"""
    if any([date_received_min == '' , date_received_max=='']) and days_ago == 0:
        return 'missing parameters'
    
    data_list = []
    list_of_states=list(requests.get("https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json").json().keys())
    for state in list_of_states:
        data= get_data_of_state(state, days_ago,'','')
        data_list.append(data)
        
    jsonObj = json.dumps(data_list)  
    ti.xcom_push(key='api_data', value=jsonObj)
    
    return 'success'


def get_data_of_state(state,days_ago, date_received_min='', date_received_max=''):
    
    server_endpoint = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'
    headers = {
    'accept': 'application/json',
    }
    if days_ago > 0:
        date_received_min, date_received_max = calculate_start_and_end_date(days_ago=days_ago)
    
    data_frame = pd.DataFrame()
    #print("state is ", state)
    params = {
            'field': 'complaint_what_happened',
            'size': '700',
            'date_received_min': date_received_min,
            'date_received_max':  date_received_max,
            'state': state
        }
    response = requests.get(url=server_endpoint, headers=headers, params=params)
    print("response is ", response)

    dict = response.json()
    
    return dict