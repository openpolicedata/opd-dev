import io
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
    bad_words = ['invalid url', 'page not found', 'token required']
    try:
        r = requests.get(source_url, headers=headers)
        txt = r.text.lower()
        imax = int(10e3)
        success = r.status_code==200 and not any(x in txt[:imax] for x in bad_words)

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
        elif success and data_type=='CSV' and source_url.lower().endswith('csv'):
            # Try reading as CSV. If unsuccessful assume bad URL
            pd.read_csv(io.StringIO(r.text))

        return success
    except requests.exceptions.SSLError:
        # Likely cannot access this site from Python
        return True
    except:
        return False
    

def extract_socrata_id_from_url(url):
    if isinstance(url, str):
        return id.group(1) if (id:=re.search(r'/([a-z\d]{4}-[a-z\d]{4})(/|$|`)', url.lower())) else None
    else:
        return url.apply(lambda x: id.group(1) if isinstance(x,str) and (id:=re.search(r'/([a-z\d]{4}-[a-z\d]{4})(/|$)', x.lower())) else None)
    

def strip_https_www(x):
    if isinstance(x, str):
        return re.sub(r'^(https?://)?(www\d?\.)?','', x).strip()
    elif isinstance(x, pd.Series):
        return x.replace(r'^(https?://)?(www\d?\.)?','', regex=True)
    elif pd.isnull(x):
        return x
    else:
        raise NotImplementedError()
    
