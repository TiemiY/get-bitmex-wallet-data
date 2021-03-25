from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import urllib.request
import urllib.parse
import time
import hashlib
import hmac
import sys
import json
import os
from os import listdir
from os.path import isfile, join
import sys

# -------------------------------------------------------------------------
# Reading paramenters from file.config
# -------------------------------------------------------------------------

def read_conf():
    param = {}
    with open('Bitmex/bitmex.conf') as f:
        for line in f:
            line = line.replace('\n', '')
            if len(line) > 0 and not line.startswith('#'):
                key = line[:line.find('=')]
                value = line[line.find('=')+1:]
                param[key] = value
    return param 

# -------------------------------------------------------------------------
# Checking mandatory paramenters
# -------------------------------------------------------------------------

def check_paramenters():
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} API_KEY API_SECRET")
        sys.exit()

# -------------------------------------------------------------------------
# Reading data from Bitmex
# -------------------------------------------------------------------------

def read_data(path):
    api_expires = int(time.time())
    api_expires += 3600

    api_key = sys.argv[1]
    api_secret = sys.argv[2]

    verb = "GET"
    message = verb + path + str(api_expires)
    api_signature = hmac.new(bytes(api_secret, 'utf8'), bytes(
        message, 'utf8'), digestmod=hashlib.sha256).hexdigest()

    headers = {
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'api-key': api_key,
        'api-expires': api_expires,
        'api-signature': api_signature
    }

    url = 'https://www.bitmex.com'+path
    req = urllib.request.Request(url, headers=headers)

    with urllib.request.urlopen(req) as f:
        bt_data = f.read()
    return bt_data

# -------------------------------------------------------------------------
# translate data to google sheets format
# -------------------------------------------------------------------------

def adjust_data(bt_data):
    bt_data = json.loads(bt_data)
    headline = []
    for key in bt_data[0]:
        headline.append(key)
    
    data = []
    data.append(headline)

    line = []
    i = 0
    for dic in bt_data:
        for key in headline:
            line.append(dic[key])
        data.append(line)
        line = []    
    return data

# -------------------------------------------------------------------------
# GS interface
# -------------------------------------------------------------------------

def GS_interface():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    creds = None
    if os.path.exists('Bitmex/token.json'):
        creds = Credentials.from_authorized_user_file(
            'Bitmex/token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'Bitmex/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('Bitmex/token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    sheet = service.spreadsheets()
    
    return sheet


# -------------------------------------------------------------------------
# API writing
# -------------------------------------------------------------------------

def write_code_GS(data, sheet, ss_ID, range):
    """ API writing """
    SAMPLE_SPREADSHEET_ID = ss_ID

    body = {
        'values': data
    }

    result = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                   range=range,
                                   valueInputOption='USER_ENTERED',
                                   body=body).execute()
    print('{} cells updated.'.format(result.get('updatedCells')))


if __name__ == '__main__':
    param = read_conf()
    check_paramenters()
    bt_data = read_data(param['path'])
    data = adjust_data(bt_data)
    sheet = GS_interface()
    write_code_GS(data, sheet, param['ss_ID'], param['range'])
