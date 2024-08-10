import os
import pandas as pd
import re
import requests
from sodapy import Socrata

headers = {'User-agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A'}

# This is for use if import data sets using Socrata. It is not required.
# Requests made without an app_token will be subject to strict throttling limits
# Get a App Token here: http://dev.socrata.com/docs/app-tokens.html
# Copy the App Token
# Create an environment variable SODAPY_API_KEY and set it equal to the API key
# Setting environment variable in Linux: https://phoenixnap.com/kb/linux-set-environment-variable
# Windows: https://www.wikihow.com/Create-an-Environment-Variable-in-Windows-10
sodapy_key = os.environ.get("SODAPY_API_KEY")

def test_url(source_url, api_url, data_type):
    try:
        r = requests.get(source_url, headers=headers)
        txt = r.text.lower()
        imax = int(10e3)
        success = r.status_code==200 and 'invalid url' not in txt[:imax] and 'page not found' not in txt[:imax]

        if success and \
            (id:=extract_socrata_id_from_url(source_url)):
            # Socrata dataset may be taken down but response will be ok
            # Test if dataset still exists

            if data_type!='Socrata':
                # Need to guess API URL from source_url
                api_url = strip_https_www(source_url)
                api_url = api_url[:api_url.find('/')]
            
            client = Socrata(api_url, sodapy_key)
            client.datasets(limit=0) # Test client. Client should always be good

            client.get_metadata(id)

        return success
    except requests.exceptions.ConnectionError:
        raise
    except:
        return False
    

def extract_socrata_id_from_url(url):
    if isinstance(url, str):
        return id.group(1) if (id:=re.search(r'/([a-z\d]{4}-[a-z\d]{4})(/|$)', url.lower())) else None
    else:
        return url.apply(lambda x: id.group(1) if isinstance(x,str) and (id:=re.search(r'/([a-z\d]{4}-[a-z\d]{4})(/|$)', x.lower())) else None)
    

def strip_https_www(x):
    if isinstance(x, str):
        return re.sub(r'^(https?://)?(www\d?\.)?','', x).strip()
    else:
        return x.replace(r'^(https?://)?(www\d?\.)?','', regex=True)
    
def update_pdap_change_type(df_opd, k, msg):
    if pd.isnull(df_opd.loc[k, "pdap_change_type"]):
        df_opd.loc[k, "pdap_change_type"] = msg
    else:
        df_opd.loc[k, "pdap_change_type"] = f'{df_opd.loc[k, "pdap_change_type"]}, {msg}'