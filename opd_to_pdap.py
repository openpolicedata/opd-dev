# %%
import os
import sys
from datetime import datetime, timedelta
import requests
import urllib
import re
import pandas as pd
from sodapy import Socrata

from pdap_utils import update_pdap_change_type, sodapy_key, test_url, extract_socrata_id_from_url, strip_https_www

import stanford

if os.path.basename(os.getcwd()) == "openpolicedata":
    opd_dir = os.path.join("..","openpolicedata")
    output_dir = os.path.join(".","data","pdap")
else:
    opd_dir = os.path.join("..","..","openpolicedata")
    output_dir = os.path.join("..","data","pdap")
sys.path.append(opd_dir)
import openpolicedata as opd

assert os.path.exists(output_dir)

kstart = 104

df_stanford = stanford.get_stanford()

us_state_to_abbrev = {"Alabama": "AL","Alaska": "AK","Arizona": "AZ","Arkansas": "AR","California": "CA","Colorado": "CO","Connecticut": "CT",
    "Delaware": "DE","Florida": "FL","Georgia": "GA","Hawaii": "HI","Idaho": "ID","Illinois": "IL","Indiana": "IN","Iowa": "IA","Kansas": "KS",
    "Kentucky": "KY","Louisiana": "LA","Maine": "ME","Maryland": "MD","Massachusetts": "MA","Michigan": "MI","Minnesota": "MN","Mississippi": "MS",
    "Missouri": "MO","Montana": "MT","Nebraska": "NE","Nevada": "NV","New Hampshire": "NH","New Jersey": "NJ","New Mexico": "NM","New York": "NY",
    "North Carolina": "NC","North Dakota": "ND","Ohio": "OH","Oklahoma": "OK","Oregon": "OR","Pennsylvania": "PA","Rhode Island": "RI","South Carolina": "SC",
    "South Dakota": "SD","Tennessee": "TN","Texas": "TX","Utah": "UT","Vermont": "VT","Virginia": "VA","Washington": "WA","West Virginia": "WV",
    "Wisconsin": "WI","Wyoming": "WY","District of Columbia": "DC","American Samoa": "AS","Guam": "GU","Northern Mariana Islands": "MP",
    "Puerto Rico": "PR","United States Minor Outlying Islands": "UM","U.S. Virgin Islands": "VI",
}

# If None, will use default file from GitHub
opd_datasets_repo = os.path.join(os.path.dirname(opd_dir), 'opd-data')
opd_source_file = None
opd_source_file = os.path.join(opd_datasets_repo, "opd_source_table.csv")
opd_deleted_file  = os.path.join(opd_datasets_repo, "datasets_deleted_by_publisher.csv")
if opd_source_file!=None:
    opd.datasets.reload(opd_source_file)
df_opd_orig = opd.datasets.query()
df_deleted = pd.read_csv(opd_deleted_file)

# Remove OPD sub-table details
df_opd_orig['TableType'] = df_opd_orig['TableType'].replace(' - .+$', '', regex=True)

# PDAP uses state abbreviations
df_opd_orig["state"] = df_opd_orig["State"].apply(lambda x: us_state_to_abbrev[x])

df_pdap_all = pd.read_csv(os.path.join(output_dir,'PDAP Data Sources_20240705.csv'))

# Code for using PDAP data source table directly from GitHub. There appear to be some issues with this table. For example, detail
# level does not have information about if a table has Individual records. On GitHub, values are federal, state, county, etc.
# PDAP sources table is a little different than the one on airtable. Needs to be merged with agences to get some of the needed values
# df_pdap["agency_described"] = df_pdap["agency_described"].apply(lambda x: x[2:-2])
# # Confirm that all agencies described match a uid
# for k,x in enumerate(df_pdap["agency_described"]):
#     if (df_pdap_agencies["airtable_uid"]==x).sum()!=1:
#         raise ValueError(f"UID in position {k}")
# df_pdap = df_pdap.merge(df_pdap_agencies, left_on="agency_described", right_on="airtable_uid", how="outer", suffixes=("","_a"))
# df_pdap["agency_described"] = df_pdap["name_a"]

# Only keep Individual record data
df_pdap = df_pdap_all[~df_pdap_all["detail_level"].isin(['Summarized totals','Aggregated records'])]

# Rename columns to PDAP
df_opd_orig.rename(columns={"Description":"description", "readme":"readme_url",
    "URL":"api_url"}, inplace=True)

# Remove items that were removed from open data sites that we reposted on GitHub
df_opd_orig = df_opd_orig[~df_opd_orig['api_url'].str.contains('github')]
df_opd_orig['source_url_type'] = 'orig'

# %%

opd_consolidated_file = r'OPD_Source_Table_consolidated.csv'
if os.path.exists(opd_consolidated_file):
    tmp = pd.read_csv(opd_consolidated_file)
    keep = [x for x in tmp.T.to_dict().values()]
else:
    keep = []

df_opd_orig['api_url_all'] = df_opd_orig['api_url']
df_opd_orig['source_url_all'] = df_opd_orig['source_url']

unused = pd.Series(True, index=df_opd_orig.index)
while unused.any():
    k = unused[unused].index[0]

    nullcurrent = pd.isnull(df_opd_orig.loc[k]['source_url'])
    for m in range(len(keep)):
        nullkeep = pd.isnull(keep[m]['source_url'])
        if keep[m]['State']==df_opd_orig.loc[k]['State'] and \
            keep[m]['SourceName']==df_opd_orig.loc[k]['SourceName'] and \
            keep[m]['TableType']==df_opd_orig.loc[k]['TableType'] and \
            ((nullkeep and nullcurrent) or \
            ((not nullkeep and 'stanford' in keep[m]['source_url']) + (not nullcurrent and 'stanford' in df_opd_orig.loc[k]['source_url'])!=1 and \
            (not nullkeep and 'muckrock' in keep[m]['source_url']) + (not nullcurrent and 'muckrock' in df_opd_orig.loc[k]['source_url'])!=1)):
            ikeep = m
            keep_exists = True
            break
    else:
        keep_exists = False
        ikeep = -1
        keep.append(df_opd_orig.loc[k].to_dict())

    matches = (df_opd_orig['State']==df_opd_orig.loc[k]['State']) & \
        (df_opd_orig['SourceName']==df_opd_orig.loc[k]['SourceName']) & \
        (df_opd_orig['TableType']==df_opd_orig.loc[k]['TableType']) & \
        (~df_opd_orig['api_url'].str.contains('stanford')) & \
        (~df_opd_orig['api_url'].str.contains('muckrock')) & \
        ('stanford' not in df_opd_orig.loc[k]['api_url']) & \
        ('muckrock' not in df_opd_orig.loc[k]['api_url'])
    
    if matches.sum()>1:
        df_matches = df_opd_orig[matches]
        for j in df_matches.index:
            if pd.notnull(df_opd_orig.loc[k]['source_url']):
                throw = pd.notnull(df_matches.loc[j, 'source_url']) and \
                    strip_https_www(df_opd_orig.loc[k]['source_url'])[:8] != strip_https_www(df_matches.loc[j, 'source_url'])[:8]
            else:
                throw = True

            if throw and strip_https_www(df_opd_orig.loc[k]['api_url'])[:8] != strip_https_www(df_matches.loc[j, 'api_url'])[:8] and \
                df_opd_orig.loc[k]['SourceName'] not in ['Fairfax County']:
                raise NotImplementedError()
            
        keep[ikeep]['coverage_start'] = df_matches['coverage_start'].min()
        keep[ikeep]['coverage_end'] = df_matches['coverage_end'].max()
        if df_matches['readme_url'].notnull().any():
                    keep[ikeep]['readme_url'] = df_matches['readme_url'][df_matches['readme_url'].notnull()].iloc[-1]
        for j in df_matches.index:
            if df_matches.loc[j, 'DataType']=='Socrata':
                assert df_matches.loc[j,'dataset_id'] in df_matches.loc[j,'source_url']
                df_matches.loc[j,'source_url'] = re.search(rf'^.+{df_matches.loc[j,'dataset_id']}', df_matches.loc[j,'source_url']).group(0)
                df_matches.loc[j, 'api_url_all'] = df_matches.loc[j, 'source_url']
        keep[ikeep]['api_url_all'] = ', '.join(df_matches['api_url_all'].unique())
        keep[ikeep]['source_url_all'] = ', '.join(df_matches['source_url_all'][df_matches['source_url_all'].notnull()].unique())
        if df_matches['DataType'].nunique()>1:
            keep[ikeep]['DataType'] = ', '.join(df_matches['DataType'].unique())

        keep[ikeep]['Agency'] = opd.defs.MULTI if (df_matches['Agency']==opd.defs.MULTI).any() else keep[ikeep]['Agency']
        
        if keep_exists:
            pass
        elif len(df_matches)>1 and df_matches['source_url'].nunique()==1 and pd.isnull(df_opd_orig.loc[k]['source_url']):
            keep[ikeep]['source_url'] = df_matches['source_url'][df_matches['source_url'].notnull()].unique()[0]
        elif df_matches['source_url'].nunique()>1:
            if (df_matches['DataType']=='Socrata').all() and df_matches['api_url'].nunique()==1:
                client = Socrata(df_opd_orig.loc[k]['api_url'], sodapy_key)
                tags = None
                success = False
                for id in df_matches['dataset_id']:
                    try:
                        meta = client.get_metadata(id)
                        success = True
                    except requests.exceptions.HTTPError:
                        print(f"Unable to access {df_opd_orig.loc[k]['api_url']} data for ID {id}")
                        continue
                    
                    if 'tags' in meta:
                        if tags:
                            tags = tags.intersection(meta['tags'])
                        else:
                            tags = set(meta['tags'])

                if not success:
                    del keep[ikeep]
                else:
                    tags = list(tags)
                    assert len(tags)>0
                    # This is the start of code for searching datasets for the least used of the common tags.
                    # However, there is an issue when search all datasets: https://stackoverflow.com/questions/78744494/socrata-find-all-datasets-from-a-domain
                    tag_counts = [0 for _ in tags]
                    # Retrieve datasets
                    r = requests.get(f'https://api.us.socrata.com/api/catalog/v1?search_context={df_opd_orig.loc[k]['api_url']}',
                                    params={'search_context':df_opd_orig.loc[k]['api_url']})
                    r.raise_for_status()
                    assert len(r.json()['results'])>0
                    for d in r.json()['results']: # Loop over datasets
                        if d['resource']['id'] in df_matches['dataset_id'].tolist():
                            continue
                        meta = client.get_metadata(d['resource']['id'])
                        for m in range(len(tags)):
                            if 'tags' in meta and tags[m] in meta['tags']:
                                tag_counts[m]+=1

                    tag_use = [t for t,c in zip(tags, tag_counts) if c==min(tag_counts)]
                    keep[ikeep]['source_url'] = f'{df_opd_orig.loc[k]['api_url']}/browse?sortBy=relevance&tags={tag_use[0]}'
                    keep[ikeep]['source_url_type'] = 'socrata tag'
            elif (isarcgis:=(df_matches['DataType']=='ArcGIS').all()) or \
                df_matches['source_url'].str.contains('.arcgis.').all():
                allowable_words = ['trafficaccidents','crime','dispatch']

                url_set = False
                if isarcgis:
                    m = re.search(r'.+\.(com|gov|org)/',df_matches['source_url'].loc[k])
                    if not m:
                        raise NotImplementedError()
                    
                    m = m.group(0)
                    target_url = re.sub(r'https?://','',m[:-1])
                    ds_url = f'https://hub.arcgis.com/api/feed/all/csv?target={target_url}'

                    # Note: The Tucson dataset does not consistently have all the arrests datasets
                    # in it leading to relaxed thresholds below
                    df_ds = pd.read_csv(ds_url)

                    tags = {}
                    not_found = 0
                    for url in df_matches['api_url']:
                        if not (m:=re.search(r'^.+/(Map|Feature)Server/\d+', url)):
                            raise NotImplementedError()
                        url_match = df_ds['url']==m.group(0)
                        if not url_match.any():
                            url_red = re.sub(r'/\d+$', '', m.group(0))
                            if not (url_match := (df_ds['url']==url_red)).any():
                                not_found+=1

                        all_tags = list(set(','.join(df_ds[url_match]['tags'][df_ds[url_match]['tags'].notnull()].tolist()).split(',')))
                        for t in all_tags:
                            if len(t)>0 and not re.search(r'^\s', t): # Tag starting with space seems to not work
                                if t in tags:
                                    tags[t]+=1
                                else:
                                    tags[t]=1

                    if not_found / len(df_matches) <= 0.5 and \
                        any(m:=[k for k,v in tags.items() if v>=len(df_matches)-not_found]):
                        popular_tags = m

                        usages = [df_ds['tags'][df_ds['tags'].notnull()].str.split(',').apply(lambda x: t in x).sum()-len(df_matches) for t in popular_tags]
                        tag = [(x,y) for x,y in zip(popular_tags,usages) if y==min(usages)]
                        
                        if tag[0][1]<=10:
                            keep[ikeep]['source_url'] = urllib.parse.urlparse(keep[ikeep]['source_url']).scheme + "://" + urllib.parse.urlparse(keep[ikeep]['source_url']).netloc + '/search?tags=' + tag[0][0]
                            keep[ikeep]['source_url_type'] = 'arcgis tag'
                            url_set = True
                    
                    if not url_set and len(tags):
                        tags = {x:v for x,v in tags.items() if len(x)>0 and \
                                (x.lower() in df_opd_orig.loc[k]['TableType'].lower() or \
                                x.lower() in allowable_words)}
                        if len(tags)>0:
                            usages = [df_ds['tags'][df_ds['tags'].notnull()].str.split(',').apply(lambda x: t in x).sum()-tags[t] for t in tags]
                            max_usages = 9
                            if min(usages)<=max_usages:
                                max_num = 0
                                for j,key in enumerate(tags):
                                    if usages[j]<=max_usages and tags[key]>max_num:
                                        max_num = tags[key]
                                        search_term = key

                                keep[ikeep]['source_url'] = urllib.parse.urlparse(keep[ikeep]['source_url']).scheme + "://" + urllib.parse.urlparse(keep[ikeep]['source_url']).netloc + '/search?q=' + search_term
                                keep[ikeep]['source_url_type'] = 'arcgis search tag'
                                url_set = True

                if not url_set:
                    cols_check = ['api_url','source_url'] if isarcgis else ['source_url']
                    max_word = []
                    for col in cols_check:
                        used = pd.Series(False, index=df_matches.index)
                        words = {}
                        for m,url in df_matches[col].items():
                            if pd.isnull(url):
                                continue
                            if col=='api_url':
                                dataset = re.search(r'/([\w\(\)]+)/(Feature|Map)Server',url).group(1)
                                if re.search(r'^[A-Za-z\d]+$',dataset):
                                    continue
                            else:
                                dataset = re.search(r'/([\w\-\:]+)/about',url).group(1)

                            used[m] = True
                            for w in re.split(r'[_-]', dataset):
                                w = w.lower()
                                w = re.sub(r'\d','', w)
                                if len(w)>0:
                                    if w in words:
                                        words[w]+=1
                                    else:
                                        words[w] = 1

                        if len(words)==0:
                            continue
                        assert max(words.values()) / len(df_matches) >= 0.5
                        max_word = [k for k,v in words.items() if v>=max(words.values())-1]
                        max_word = [x for x in max_word if x in df_opd_orig.loc[k]['TableType'].lower() or x in allowable_words]
                        if len(max_word)>0:
                            break
                    else:
                        max_word = [df_opd_orig.loc[k]['TableType'].title()]
                    assert len(max_word)>0
                    keep[ikeep]['source_url'] = urllib.parse.urlparse(keep[ikeep]['source_url']).scheme + "://" + urllib.parse.urlparse(keep[ikeep]['source_url']).netloc + '/search?q=' + max_word[0]
                    keep[ikeep]['source_url_type'] = 'arcgis url parse'
            elif (df_matches['DataType'].isin(['Excel','CSV'])).all():
                url_matches_last = df_matches['source_url']==df_matches['source_url'].iloc[-1]
                if df_matches['source_url'].nunique() / len(df_matches) < 0.25 and url_matches_last.sum()>1 and \
                    url_matches_last.loc[[k for k,x in url_matches_last.items() if x][0]:].all():  # Check that all URLs at the end are the same
                    keep[ikeep]['source_url'] =  df_matches['source_url'].iloc[-1]
                    keep[ikeep]['source_url_type'] = 'csv latest'
                else:
                    min_url = [x for x in df_matches['source_url'] if len(x)== min([len(x) for x in df_matches['source_url']])][0]
                    min_url = min_url if min_url[-1]!='/' else min_url[:-1]
                    if df_matches['source_url'].str.contains(min_url).all():
                        keep[ikeep]['source_url'] =  min_url
                        keep[ikeep]['source_url_type'] = 'csv common'
                    else:
                        raise NotImplementedError()
            else:
                raise NotImplementedError()

    unused.loc[k] = False
    unused.loc[matches[matches].index] = False

df_opd_red = pd.DataFrame(keep)
df_opd_red.to_csv(opd_consolidated_file, index=False)

# %%

# Allow these PDAP URLs to change to OPD. No issue with these URLS. OPD URL may be more general.
alternative_urls = ['https://data.lacounty.gov/datasets/lacounty::sheriff-all-shooting-incidents-for-deputy-involved-shootings-2010-to-present-deputy-shootings/about'
                    ]

# These are tables whose type sometimes differs between PDAP and OPD. Assuming OPD is more accurate.
eq_table_types = [
    ('Incident Reports', 'Pointing Weapon'),
    ('Crime Statistics', 'Incident Reports'),
    ('Incident Reports', 'Accident Reports'),
    ('Stops', 'Traffic Stops'),
]

allowable_record_format_changes = [('CSV','Excel')]

# Convert table type from all caps to PDAP format
df_opd_red["record_type"] = df_opd_red["TableType"].apply(lambda x: x.title())
# Convert OPD record type names to PDAP
opd_to_pdap_record_types = {
    "Arrests" : "Arrest Records", 
    "Calls For Service":"Calls for Service",
    "Complaints":"Complaints & Misconduct",
    'Crashes':'Accident Reports',
    'Employee':'Personnel Records',
    'Incidents':'Incident Reports',
    'Lawsuits':'Court Cases',
    "Officer-Involved Shootings":"Officer Involved Shootings", 
    'Traffic Citations':'Citations',
    "Use Of Force" : "Use of Force Reports",
    }
df_opd_red["record_type"] = df_opd_red["record_type"].apply(lambda x: opd_to_pdap_record_types[x] if x in opd_to_pdap_record_types else x)

# Check that all record type mappings worked
for k,v in opd_to_pdap_record_types.items():
    if v not in df_opd_red["record_type"].unique():
        raise KeyError(f"Key {k} not found in OPD record types to update to PDAP type {v}")

df_opd = df_opd_red.copy()

# Initialize columns
# possible_pdap_name_match is the name of a source from PDAP that might be a match
init_to_empty = ["data_portal_type", "agency_aggregation", "access_type",
    "record_format", "supplying_entity",
    "agency_originated","possible_pdap_name_match",'pdap_change_type']
for x in init_to_empty:
    df_opd[x] = pd.NA
    
# May need to update this in the future if we ever have non-agency supplied data
init_to_true = []
for x in init_to_true:
    df_opd[x] = True

df_opd["agency_supplied"] = "yes"

# All our data are individual records
df_opd["detail_level"] = "Individual record"
df_opd["scraper_url"] = "https://pypi.org/project/openpolicedata/"

# access_type can currently only be API or web page. Should something like Downloadable File be available for files to indicate that the data doesn't 
# have to be scraped or copied from a web page? We're including Downloadable File as an option here in case someone wants to use it
download = "Download"
data_type_to_access_type = {"Socrata":"API,"+download, "ArcGIS":"API,"+download,
    "CSV":download,"Excel":download,"Carto":"API,"+download,'HTML':'Web page',
    "CKAN":"API,"+download}

bad_pdap_entries = []
for k in df_opd.index:
    if k<kstart:
        continue

    print("{}: {} {}".format(k, df_opd.loc[k, "SourceName"], df_opd.loc[k, "record_type"]))

    opd_source_url = strip_https_www(df_opd.loc[k,'source_url'])
    opd_api_urls = [strip_https_www(y.strip()) for y in df_opd.loc[k, 'api_url_all'].split(',')]
    opd_source_urls = [strip_https_www(y.strip()) for y in df_opd.loc[k, 'source_url_all'].split(',')]
    
    src = opd.Source(df_opd.loc[k, "SourceName"], df_opd.loc[k, "State"])

    data_types = df_opd.loc[k, "DataType"].split(',')
    access_types = [data_type_to_access_type[x.strip()] for x in data_types]
    df_opd.loc[k, "access_type"] = ",".join(set(access_types))

    if 'stanford' in df_opd.loc[k, 'api_url']:
        # Stanford has already been added. Ensure that this dataset is included in the agency_described
        stanford_pdap = df_pdap[df_pdap['name'].str.lower().str.contains('stanford')]
        assert len(stanford_pdap)==1
        stanford_agencies = stanford_pdap.iloc[0]['agency_described'].split(',')
        stanford_states = stanford_pdap.iloc[0]['state'].split(',')
        matches = [x==df_opd.loc[k, 'state'] and y.startswith(df_opd.loc[k, "AgencyFull"]) for x,y in zip(stanford_states, stanford_agencies)]

        if any(matches):
            # Already added 
            df_opd.loc[k, 'airtable_uid'] = stanford_pdap.iloc[0]['airtable_uid']
            continue
        else:
            state_matches = [y for x,y in zip(stanford_states, stanford_agencies) if x==df_opd.loc[k, 'state']]
            if any(['Aggregated' in x for x in state_matches]) and \
                any([x in df_opd.loc[k, "AgencyFull"] for x in ['Patrol Division','State Police','State Patrol','Highway Patrol']]):
                df_opd.loc[k, 'airtable_uid'] = stanford_pdap.iloc[0]['airtable_uid']
                update_pdap_change_type(df_opd, k, 'stanford_state_patrol_not_aggregated')
                continue
            else:
                raise NotImplementedError()

    
    if 'muckrock' in df_opd.loc[k, 'api_url']:
        raise NotImplementedError()

    portal_type = [data_types[k] for k in range(len(data_types)) if access_types[k]!=download]
    df_opd.loc[k, "data_portal_type"] = ",".join(set(portal_type))

    # PDAP uses XLS instead of Excel
    record_format = [data_types[k].replace('Excel','XLS') for k in range(len(data_types)) if access_types[k]==download]
    df_opd.loc[k, "record_format"] = ",".join(set(record_format))

    eq_urls = [('data-cotgis.opendata.arcgis.com','gisdata.tucsonaz.gov')]

    assert not any('github' in x for x in opd_api_urls)

    is_bad_response = None
    is_api_url = None
    if df_opd.loc[k, "Agency"] == opd.defs.MULTI:
        df_opd.loc[k, "agency_originated"] = "yes"
        df_opd.loc[k, "agency_supplied"] = "no"
        df_opd.loc[k, "agency_aggregation"] = "state"
        df_opd.loc[k, "agency_described"] = df_opd.loc[k, "State"] + " Aggregated - " + df_opd.loc[k, "state"]
        # This is the best that we can do right now. It really should be the name of the state agency that publishes the data
        if df_opd.loc[k, "State"]=="Virginia":
            df_opd.loc[k, "supplying_entity"] = "Virginia State Police"
        elif df_opd.loc[k, "State"]=="California":
            df_opd.loc[k, "supplying_entity"] = "California Department of Justice"
        elif df_opd.loc[k, "State"]=="New Jersey":
            df_opd.loc[k, "supplying_entity"] = "New Jersey Office of the Attorney General"
        elif df_opd.loc[k, "State"]=="Connecticut":
            df_opd.loc[k, "supplying_entity"] = "Connecticut Racial Profiling Prohibition Project"
        else:
            raise ValueError("Unknown multi-agency")
        
        pdap_matches_agency = df_pdap[df_pdap["agency_described"] == df_opd.loc[k, "agency_described"]]
        assert len(pdap_matches_agency)>0

        pdap_matches = pdap_matches_agency[pdap_matches_agency["record_type"]==df_opd.loc[k, "record_type"]]

        if len(pdap_matches)==0 and \
            (m:=pdap_matches_agency['record_type'].apply(lambda x: any([x==y and z==df_opd.loc[k, "record_type"] for y,z in eq_table_types]))).any():
            pdap_matches = pdap_matches_agency[m]
            update_pdap_change_type(df_opd, k, 'record_type_recommendation')

        if len(pdap_matches)==0 and df_opd.loc[k, "record_type"]=='Deaths In Custody':
            update_pdap_change_type(df_opd, k, 'New')
            continue
        elif len(pdap_matches)>0:
            is_api_url = pd.Series(False, pdap_matches.index)
            for idx in pdap_matches.index:          
                pdap_url = strip_https_www(pdap_matches.loc[idx, 'source_url'])    
                is_api_url[idx] = pdap_url in opd_api_urls or pdap_matches.loc[idx, 'source_url'].endswith('.json')

            if is_api_url.all():
                df_opd.loc[k, 'airtable_uid'] = pdap_matches.iloc[0]['airtable_uid']
                update_pdap_change_type(df_opd, k, 'api_to_source_url')
                for idx in is_api_url.index[1:]:
                    bad_pdap_entries.append(pdap_matches.loc[idx])
                    continue

                continue
            else:
                raise NotImplementedError()
        else:
            raise NotImplementedError()
    else:
        # Get PDAP rows for this state and record type
        pdap_matches_agency = df_pdap[(df_pdap["state"]==df_opd.loc[k, "state"]) & \
                                    df_pdap["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"])]
        if len(pdap_matches_agency)==0:
            pdap_matches_agency2 = df_pdap[(df_pdap["state"]==df_opd.loc[k, "state"]) & \
                                    df_pdap["agency_described"].str.startswith(df_opd.loc[k, "Agency"])] 
            assert len(pdap_matches_agency2)==0

            if len(pdap_matches_agency2)==0:
                update_pdap_change_type(df_opd, k, 'New')
                continue

        pdap_matches = pdap_matches_agency[pdap_matches_agency["record_type"]==df_opd.loc[k, "record_type"]]

    if len(pdap_matches)==0:
        pdap_urls = strip_https_www(pdap_matches_agency['source_url'])
        pdap_matches = pdap_matches_agency[pdap_urls==opd_source_url]
        if len(pdap_matches)==0:
            # Try to clean PDAP URL
            pdap_urls = pdap_urls.replace(r'`$', '', regex=True)
            pdap_matches = pdap_matches_agency[pdap_urls==opd_source_url]
            if len(pdap_matches)>0:
                update_pdap_change_type(df_opd, k, 'source_url_typo_correction')
                pdap_matches.loc[:, 'source_url'] = opd_source_url
            elif df_opd.loc[k, 'DataType']=='Socrata' and (id:=extract_socrata_id_from_url(opd_source_url)) and \
                ((match:=extract_socrata_id_from_url(pdap_matches_agency.loc[:, 'source_url'])==id)).any():
                pdap_matches = pdap_matches_agency[match]

            if len(pdap_matches)==0:
                # Try to match to API URLs
                pdap_matches = pdap_matches_agency[pdap_urls.isin(opd_api_urls)]

        if len(pdap_matches)>0 and \
            (m:=pdap_matches['record_type'].apply(lambda x: any([x==y and z==df_opd.loc[k, "record_type"] for y,z in eq_table_types]))).any():
            pdap_matches = pdap_matches[m]
            update_pdap_change_type(df_opd, k, 'record_type_recommendation')
        else:
            update_pdap_change_type(df_opd, k, 'New')
            continue
        
    if len(pdap_matches)>1 and len(strip_https_www(pdap_matches['source_url']).unique())==1 and \
        (m:=pdap_matches['scraper_url'].str.contains('openpolicedata', na=False)).sum()==1:
        # Multiple entries have the same URL. Use the one with OPD as the scraper_url
        for idx, v in m.items():
            if not v:
                bad_pdap_entries.append(pdap_matches.loc[idx])

        pdap_matches = pdap_matches[m]

    if len(pdap_matches)>1:
        is_null = pd.Series(False, pdap_matches.index)
        is_scraper = pd.Series(False, pdap_matches.index)
        source_equal = pd.Series(False, pdap_matches.index)
        bad_response = pd.Series(False, pdap_matches.index)
        is_api_url = pd.Series(False, pdap_matches.index)
        is_source_url = pd.Series(False, pdap_matches.index)
        for idx in pdap_matches.index:          
            is_null[idx] = pd.isnull(pdap_matches.loc[idx, 'source_url'])
            is_scraper[idx] = pd.notnull(pdap_matches.loc[idx, 'scraper_url']) and 'openpolicedata' in pdap_matches.loc[idx, 'scraper_url']
            if is_null[idx]:
                continue

            pdap_url = strip_https_www(pdap_matches.loc[idx, 'source_url'])  
            source_equal[idx] = pdap_url==opd_source_url  
            is_api_url[idx] = pdap_url in opd_api_urls
            is_source_url[idx] = pdap_url in opd_source_urls

            if not source_equal[idx]:  # Assuming OPD source URls are good, avoids bad responses due to site maintenance, etc.
                bad_response[idx] = not test_url(pdap_matches.loc[idx, 'source_url'], df_opd.loc[k, 'api_url'], df_opd.loc[k, 'DataType'])

            # Need to add all api_urls to consolidated api url

        assert not is_null.all()

        for idx, v in is_null.items():
            if v:
                bad_pdap_entries.append(pdap_matches.loc[idx])

        pdap_matches = pdap_matches[~is_null]
        bad_response = bad_response[~is_null]
        is_api_url = is_api_url[~is_null]

        if len(pdap_matches)>1 and not bad_response.all():
            for idx, v in bad_response.items():
                if v:
                    bad_pdap_entries.append(pdap_matches.loc[idx])

            pdap_matches = pdap_matches[~bad_response]
            is_api_url = is_api_url[~bad_response]
            bad_response = bad_response[~bad_response]

        if len(pdap_matches)>1:
            if not is_api_url.all():
                for idx, v in is_api_url.items():
                    if v:
                        bad_pdap_entries.append(pdap_matches.loc[idx])

                pdap_matches = pdap_matches[~is_api_url]
                bad_response = bad_response[~is_api_url]
                is_api_url = is_api_url[~is_api_url]
                update_pdap_change_type(df_opd, k, 'removed_api_urls_from_pdap')
            elif is_api_url.all():
                # Just keep the first one
                for idx in is_api_url.index[1:]:
                    bad_pdap_entries.append(pdap_matches.loc[idx])

                pdap_matches = pdap_matches.iloc[0:1]
                bad_response = bad_response.iloc[0:1]
                is_api_url = is_api_url.iloc[0:1]
                update_pdap_change_type(df_opd, k, 'inclusive_source_URL')

            
        if bad_response.all():
            # Just keep the first one
            for idx in bad_response.index[1:]:
                bad_pdap_entries.append(pdap_matches.loc[idx])

            pdap_matches = pdap_matches.iloc[0:1]
            bad_response = bad_response.iloc[0:1]
            is_api_url = is_api_url.iloc[0:1]
            update_pdap_change_type(df_opd, k, 'current_source_url_dead')

        is_bad_response = bad_response.iloc[0]
        is_api_url = is_api_url.iloc[0]
    elif len(pdap_matches)==1:
        is_api_url = strip_https_www(pdap_matches.iloc[0]['source_url']) in opd_api_urls

        if len(pdap_matches)>1:
            raise NotImplementedError()


    if len(pdap_matches)==1:
        already_added = True
        if pd.notnull(pdap_matches.iloc[0]['scraper_url']):
            if 'openpolicedata' not in pdap_matches.iloc[0]['scraper_url']:
                raise NotImplementedError()
        else:
            already_added = False
            update_pdap_change_type(df_opd, k, 'scraper_url_added')

        eq_url_test = False
        if strip_https_www(pdap_matches.iloc[0]['source_url']) != opd_source_url:
            if already_added and len(opd_api_urls)==1:
                # Assuming that PDAP already added the Source URL that they wanted
                pass
            elif len(opd_api_urls)>1 and strip_https_www(pdap_matches.iloc[0]['source_url']) in opd_api_urls:
                # Current Source URL is likely a single dataset of a table that spans multiple datasets. Use OPD source URL
                update_pdap_change_type(df_opd, k, 'inclusive_source_URL')
            elif any([x in pdap_matches.iloc[0]['source_url'] and y in opd_source_url for x,y in eq_urls]) and \
                test_url(pdap_matches.iloc[0]['source_url'], df_opd.loc[k,'api_url'], df_opd.loc[k,'DataType']):
                # Likely changed or equivalent URL
                eq_url_test = True
            elif is_bad_response or (is_bad_response==None and \
                                     not test_url(pdap_matches.iloc[0]['source_url'], df_opd.loc[k,'api_url'], df_opd.loc[k,'DataType'])):
                update_pdap_change_type(df_opd, k, 'current_source_url_dead')
            elif is_api_url:
                pass
            elif pdap_matches.iloc[0]['source_url'] in alternative_urls:
                update_pdap_change_type(df_opd, k, 'replace_current_source_url')
            elif df_opd.loc[k,'TableType']=='INCIDENTS':
                # PDAP Incident Reports is currently a bit of a catch all. Assuming this is a new dataset.
                update_pdap_change_type(df_opd, k, 'New')
                continue
            else:
                update_pdap_change_type(df_opd, k, 'current_source_url_may_be_adequate')
            
        if pd.isnull(pdap_matches.iloc[0]['readme_url']) and pd.notnull(df_opd.loc[k, 'readme_url']):
            update_pdap_change_type(df_opd, k, 'readme_url_added')
            
        if (ds1:=pd.to_datetime(pdap_matches.iloc[0]['coverage_start'])) != (ds2:=pd.to_datetime(df_opd.loc[k, 'coverage_start'])) and \
            (pd.notnull(ds1) or pd.notnull(ds2)):
            update_pdap_change_type(df_opd, k, 'coverage_start_updated')
            
        if (de1:=pd.to_datetime(pdap_matches.iloc[0]['coverage_end'])) != (de2:=pd.to_datetime(df_opd.loc[k, 'coverage_end'])) and \
            (pd.notnull(de1) or (pd.notnull(de2) and de2.year!=2024)):  # PDAP appears to use null for data that should be up-to-date
            update_pdap_change_type(df_opd, k, 'coverage_end_updated')
            
        if pdap_matches.iloc[0]['access_type'] != df_opd.loc[k, "access_type"]:
            if 'API' in df_opd.loc[k, "access_type"].split(',') and \
                'API' not in pdap_matches.iloc[0]['access_type']:
                update_pdap_change_type(df_opd, k, 'access_type_corrected')
            elif all([x in pdap_matches.iloc[0]['access_type'].split(',') for x in df_opd.loc[k, "access_type"].split(',')]):
                pass
            else:
                raise NotImplementedError()
            
        if pdap_matches.iloc[0]['data_portal_type'] != df_opd.loc[k, "data_portal_type"] and \
            not (pdap_matches['data_portal_type'].isnull().iloc[0] and df_opd.loc[k, 'data_portal_type']==''):
            if pd.isnull(pdap_matches.iloc[0]['data_portal_type']) and 'API' in df_opd.loc[k, "access_type"].split(',') and \
                (eq_url_test or 
                ((m1:=re.search(r'[\w\.]+/',pdap_matches.iloc[0]['source_url'])) and (m2:=re.search(r'[\w\.]+/',opd_source_url)) and \
                re.sub(r'www\d?\.','',m1.group(0))==re.sub(r'www\d?\.','',m2.group(0)))):
                pass
            elif pd.isnull(pdap_matches.iloc[0]['data_portal_type']) and pd.isnull(pdap_matches.iloc[0]['record_format']):
                update_pdap_change_type(df_opd, k, 'data_portal_type_corrected')
            else:
                raise NotImplementedError()

        if len(df_opd.loc[k, "record_format"])>0 and pdap_matches.iloc[0]['record_format'] != df_opd.loc[k, "record_format"]:
            if any([pdap_matches.iloc[0]['record_format']==y and z==df_opd.loc[k, "record_format"] for y,z in allowable_record_format_changes]):
                update_pdap_change_type(df_opd, k, 'record_format_corrected')
            else:
                raise NotImplementedError()
        
        if pdap_matches.iloc[0]['detail_level'] != 'Individual record':
            if pd.isnull(pdap_matches.iloc[0]['detail_level'] ):
                update_pdap_change_type(df_opd, k, 'detail_level_added')
            else:
                raise NotImplementedError()
        
        df_opd.loc[k, 'airtable_uid'] = pdap_matches.iloc[0]['airtable_uid']
    else:
        raise NotImplementedError()
    
    continue

    pdap_matches = pdap_matches[~pdap_matches["source_url"].str.endswith(".pdf")]
    if df_opd.loc[k, "Agency"] == opd.defs.MULTI:
        pdap_matches = pdap_matches[pdap_matches["agency_described"] == df_opd.loc[k, "agency_described"]]
    else:
        pdap_matches = pdap_matches[pdap_matches["agency_described"].str.startswith(df_opd.loc[k, "SourceName"])]
        if len(pdap_matches)>0:
            pdap_matches_orig = pdap_matches.copy()
            pdap_matches = pdap_matches[pdap_matches["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"])]
            if len(pdap_matches)==0:
                pdap_matches = pdap_matches_orig[pdap_matches_orig["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"].replace(" Department",""))]
                if len(pdap_matches)==0:
                    raise ValueError("Check PDAP match")
        if len(pdap_matches)==1:
            pass
            # if "County" in df_opd.loc[k, "SourceName"] and pdap_matches["county"].iloc[0]!=df_opd.loc[k, "SourceName"]:
            #     raise ValueError("county does not match")
            # elif pdap_matches["municipality"].iloc[0]!=df_opd.loc[k, "SourceName"] and df_opd.loc[k, "SourceName"] not in ["Austin","Seattle"]: # Austin listed as Cedar Park for some reason
            #     raise ValueError("municipality does not match")
        elif len(pdap_matches)>1:
            # Check if all source URLs are the same
            if (pdap_matches["source_url"]==pdap_matches["source_url"].iloc[0]).all():
                print("{} matches for {} table for {}".format(len(pdap_matches), pdap_matches["record_type"].iloc[0], pdap_matches["agency_described"].iloc[0]))
                pdap_matches = pdap_matches.iloc[0].to_frame().transpose()
            else:
                if pd.notnull(df_opd.loc[k, "dataset_id"]):
                    pdap_matches = pdap_matches[pdap_matches["source_url"].str.contains(df_opd.loc[k, "dataset_id"])]
                ignore = False
                if len(pdap_matches)==0 and df_opd.loc[k, "SourceName"]=="Philadelphia" and df_opd.loc[k, "readme_url"]!=None:
                    url_matches = pdap_matches["source_url"].str.contains(urllib.parse.urlparse(df_opd.loc[k, "readme_url"]).netloc)
                    if url_matches.any():
                        raise NotImplementedError("Not handling this case yet")
                    else:
                        ignore = True

                if len(pdap_matches)!=1 and not ignore:
                    raise ValueError("Unexpected # of matches")
    
    if len(pdap_matches)==1:
        df_opd.loc[k, "possible_pdap_name_match"] = pdap_matches["name"].iloc[0]
    elif len(pdap_matches)>1:
        raise ValueError("Unexpected # of matches")


# Drop OPD columns
df_opd.drop(columns=["State","SourceName","Agency","TableType","Year","DataType","date_field","dataset_id","agency_field","min_version"], inplace=True)

# Resort columns to match PDAP
new_cols = [x for x in df_pdap.columns if x in df_opd.columns]
extra_cols = [x for x in df_opd.columns if x not in new_cols]
new_cols.extend(extra_cols)

df_opd = df_opd[new_cols]

output_file = os.path.join(output_dir, "OPD_PDAP_Submission_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".csv")
df_opd.to_csv(output_file, index=False)

# TODO: Add code standardizing OPD scraper_url info
# TODO: Incident Reports record type is a catch all with many corrections needed