import json
import requests
import pandas as pd
from googleapiclient.discovery import build
from datetime import date, timedelta
from google.oauth2 import service_account
from moduleetl.sheets import *

def get_gcp_credentials():
    """creating credentials object for authentication"""
    print('getting credientials')
    # service_acc = open('ssh/creds_temp.json', 'r').read()
    service_acc = '''
    {
  "type": "service_account",
  "project_id": "inbound-summit-341812",
  "private_key_id": "d4cd212c2cbee3fb977d07c193b15a72facf4599",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDOdrxC1ZbGF7Tf\nHqRJ+Xz7/dfrMv5S5d32yzmGERdTSTspp+50hiYN0JK3SPUzOKgAQLEWHvg92oG2\nfOAJB8yVylIhYWuWMXLYAaAs2B9eaEUTEG+KITuxuXtEZW7T2+DGZ774uJ46f9+R\n8g3XD/vfrQME/S/gyJYMKbFo5C87wu/ydySsLrnzblX67lEeL5gHjagPI+AfGl7j\nhNiXAqSeOGADyRXtuuBHOjbkQb3MTbpXqcW3ETi07zto9vY55lZkZG1uZ5DXa5Pd\nj5qK3IrQXS/8M5rOU7URIjrnqWAQN+xETnVF3e2REmOUoQlMFFGgyG+Q7M+U5p9S\n6edYp9r1AgMBAAECggEAG0Ig2+AQLmod1qFYhm26exiKA8LxQeeYVQeCujkSb8wI\n1BiYSmkVqBhD7o/i05bk01e3KZkAJEZvfrTg7UBK2fHiwwZ2Klev9mXMjG8iQRga\nGoi+eSmnFHFNHXwY1D7A3+D9nR/ewBF25nFjM2QoWGEit2toAVDqQoPUqOkLdWOs\nDcM1e0uJukt21LMHTTKdfcV276/2+4mhLEagUeCVP9F/UomIYp+hCcIrISPd+hVS\nTAg7UCMLNAWSXG/YZFvnUlcqzBneJRvdt2S40Ml+je8WLwPtqlSBhwUvnh1yfmUd\nyvoiYlWiN7/QuXiYNMv1lV9w4RZ8ajPWfSKW6RSeFwKBgQDoE8fJr6p+C0+1wQjI\n50U6j7feeiIe5N/N92EL28cNnuIitpwYGMnj+fiYVc89xERno+wMlDWEIj+Odi6S\nYks/xvWW64ONIUj1AxEyQH2XwYJjXzWitW9pZurxO+Dg3SQ3y80JdlSW1VEaHnuz\nk0a0RDWlDtx2nflFwEUXwdRWEwKBgQDjvwyEYNSAnL3UMMm8OLFzArtUpoKo2Qwq\n8H67ZLV/pDtTecwF47v9ovLNqhERNyUFFd1uF9OaciW7AHmxsaMdpkAK0+E2FW8f\nV2V5a8exBbkU5A5p/7VUqJUWtq8bsFvvhfR1dPh+tWwm0lf/PikwgHnuP4Mk8gId\nOzZgcPRL1wKBgE31bxSKJQyIQjN877WFMtU0OADNKC8WlTO8GOaB4oDp0W71xsO1\neARUETUjBiXqi0wBXabwFnYXhyAVBRn5WOvIqvESljNPbhGYASvPTCExSNqWvg61\nQgK3Js74XVdMEgGIPY0jKdEtQTwz5zvwijy9+QNEVbym3niW2N7dJrXvAoGBAKOu\n5dImvdQJ0y00O2l53Z9pcTXvzqyIAoTCAHOyd/fbE4B1YztAaBZtDNDf0BBtb7/t\n7Q15Nm4kaBW0y8H4h+GBYTnm+lAiq5D9AUN5QTZrhJANMIbib3L/DtgaSFRANnM5\n9W6NuSNoROjJ+NDL3lKro/al0x20iex0aCp8RDhVAoGAEMoQfyQnPcDbLrcgY13J\nPNsgwc/76A79xgNpl8OcoUgqrtfXmAq7JqBzCtbCXrmkQohqZGcfGPScekCo8qps\nHZH0RNG8zyasFSQaJdXvp74dA4ICInW/I/GCu2jycE+Z+6RrOV3xvemS/jd5cQPY\nB4xFiVs91JFWDs2G7fjr/p0=\n-----END PRIVATE KEY-----\n",
  "client_email": "week4-batch2@inbound-summit-341812.iam.gserviceaccount.com",
  "client_id": "102166575559931792583",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/week4-batch2%40inbound-summit-341812.iam.gserviceaccount.com"
}

    '''
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(service_acc, strict=False),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return credentials

def upload_data_into_sheet(ti):
    complete_data = pd.read_csv ('transform.csv')
    
    """ complete_date = pd.DataFrame()
    for doc in data_json['hits']['hits'][:5]:
        doc_updated = doc['_source']
        doc_updated.update({'_id':doc['_id']})
        # print()
        # print(doc_updated)
        complete_date = complete_date.append(doc_updated, ignore_index=True) """
    credentials = get_gcp_credentials()
    sheet_client = build(
        'sheets', 'v4', 
        credentials=credentials,
        cache_discovery=False)
    range_name = 'states'
    sheet_id = '1Stei88CUwjCGuBT1u9r9a6Cbt0Lt7e68r5Eyh5yRpA8'
    columns = list(complete_data.columns)
    values = [list(i) for i in complete_data.itertuples(index=False)]
    # values = [['col1', 'col2'],['one', 'two']]
    # getting data from sheet to check if data is already there or not
    results = get_sheet_data(sheet_client, sheet_id, range_name)
    if results:
        if not results.get('values'):
            values.insert(0, columns)
    update_results = update_sheet_data(sheet_client, sheet_id, values, range_name)
    return 'success'