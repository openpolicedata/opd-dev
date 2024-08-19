import pandas as pd
import re

from pdap_utils import strip_https_www, extract_socrata_id_from_url, test_url

def find_equivalent_record_type(df_pdap, df_opd, k):
    # These are tables whose type sometimes differs between PDAP and OPD. Assuming OPD is more accurate.
    eq_table_types = [
        ('Incident Reports', 'Pointing Weapon'),
        ('Crime Statistics', 'Incident Reports'),
        ('Incident Reports', 'Accident Reports'),
    ]

    pdap_matches = []
    if (m:=df_pdap['record_type'].apply(lambda x: any([x==y and z==df_opd.loc[k, "record_type"] for y,z in eq_table_types]))).any():
        pdap_matches = df_pdap[m]
        df_opd.loc[k, 'update_record_type'] = True
        df_opd.loc[k, 'pdap_record_type'] = ', '.join(pdap_matches['record_type'].tolist())

    return pdap_matches, df_opd


def match_stanford(df_pdap, df_opd, k):
    # Stanford has already been added. Ensure that this dataset is included in the agency_described
    stanford_pdap = df_pdap[df_pdap['name'].str.lower().str.contains('stanford')]
    assert len(stanford_pdap)==1
    stanford_agencies = stanford_pdap.iloc[0]['agency_described'].split(',')
    stanford_states = stanford_pdap.iloc[0]['state'].split(',')
    if df_opd.loc[k, "Agency"]=='MULTIPLE':
        matches = [y for x,y in zip(stanford_states, stanford_agencies) if x==df_opd.loc[k, 'state']]
        if len(matches)>0:
            if not any(x==f"{df_opd.loc[k, 'State']} Aggregated - {df_opd.loc[k, 'state']}" for x in matches):
                raise NotImplementedError()
    else:
        matches = [x==df_opd.loc[k, 'state'] and y.startswith(df_opd.loc[k, "AgencyFull"]) for x,y in zip(stanford_states, stanford_agencies)]

    if not any(matches):
        state_matches = [y for x,y in zip(stanford_states, stanford_agencies) if x==df_opd.loc[k, 'state']]
        if len(state_matches)>0:
            df_opd = check_aggregation(state_matches, df_opd, k)
        else:
            df_opd.loc[k, 'stanford_new'] = True
        
    df_opd.loc[k, 'airtable_uid'] = stanford_pdap.iloc[0]['airtable_uid']
        
    return df_opd

def match_multi(df_pdap, df_opd, k):
    df_opd.loc[k, "agency_originated"] = "yes"
    df_opd.loc[k, "agency_supplied"] = "no"
    df_opd.loc[k, "agency_aggregation"] = "state"
    df_opd.loc[k, "Agency"] = df_opd.loc[k, "State"] + " Aggregated - " + df_opd.loc[k, "state"]
    # This is the best that we can do right now. It really should be the name of the state agency that publishes the data
    if df_opd.loc[k, "State"]=="Virginia":
        df_opd.loc[k, "supplying_entity"] = "Virginia State Police"
    elif df_opd.loc[k, "State"]=="California":
        df_opd.loc[k, "supplying_entity"] = "California Department of Justice"
    elif df_opd.loc[k, "State"]=="New Jersey":
        df_opd.loc[k, "supplying_entity"] = "New Jersey Office of the Attorney General"
    elif df_opd.loc[k, "State"]=="Connecticut":
        df_opd.loc[k, "supplying_entity"] = "Connecticut Racial Profiling Prohibition Project"
    elif df_opd.loc[k, "State"]=="Massachusetts":
        df_opd.loc[k, "supplying_entity"] = "Massachusetts POST Commission"
    elif df_opd.loc[k, "State"]=="New York":
        df_opd.loc[k, "supplying_entity"] = "New York DMV"
    else:
        raise ValueError("Unknown multi-agency")
    
    pdap_matches_agency = df_pdap[df_pdap["agency_described"] == df_opd.loc[k, "Agency"]]

    pdap_matches = pdap_matches_agency[pdap_matches_agency["record_type"]==df_opd.loc[k, "record_type"]] if len(pdap_matches_agency)>0 else []

    if len(pdap_matches)==0:
        pdap_matches, df_opd = find_equivalent_record_type(pdap_matches_agency, df_opd, k)

    if len(pdap_matches)==0:
        df_opd.loc[k, 'new'] = True
    elif len(pdap_matches)>0:
        df_opd = compare_values(pdap_matches, df_opd, k)

        if (pdap_matches['agency_originated'] != "yes").any():
            df_opd.loc[k, 'update_agency_originated'] = "newvalue: yes"

        if (pdap_matches['agency_supplied'] != "no").any():
            df_opd.loc[k, 'update_agency_supplied'] = "newvalue: no"

        if (pdap_matches['agency_aggregation'] != "state").any():
            df_opd.loc[k, 'update_agency_aggregation'] = "state"

        if (pdap_matches['supplying_entity'] != df_opd.loc[k, "supplying_entity"]).any():
            df_opd.loc[k, 'update_supplying_entity'] = df_opd.loc[k, "supplying_entity"]

        df_opd.loc[k, 'airtable_uid'] = ', '.join(pdap_matches['airtable_uid'].tolist())
    
    return df_opd

def match(df_pdap, df_opd, k):
    opd_source_url = strip_https_www(df_opd.loc[k,'source_url'])
    # Get PDAP rows for this state and record type
    pdap_matches_agency = df_pdap[(df_pdap["state"]==df_opd.loc[k, "state"]) & \
                                df_pdap["agency_described"].str.startswith(df_opd.loc[k, "AgencyFull"])]
    if len(pdap_matches_agency)==0:
        pdap_matches_agency2 = df_pdap[(df_pdap["state"]==df_opd.loc[k, "state"]) & \
                                df_pdap["agency_described"].str.startswith(df_opd.loc[k, "Agency"])] 

        if len(pdap_matches_agency2)==0:  # Nothing found for this agency
            df_opd.loc[k, 'new'] = True
            return df_opd
        else:
            pdap_matches_agency = pdap_matches_agency2

    if df_opd.loc[k, "record_type"]=='Incident Reports':
        pdap_matches = []  # This record_type name is used too frequently in PDAP to be reliable. Need to match URL
    else:
        pdap_matches = pdap_matches_agency[pdap_matches_agency["record_type"]==df_opd.loc[k, "record_type"]]

    if len(pdap_matches)==0:
        pdap_matches, df_opd = match_source_url(pdap_matches_agency, df_opd, k, opd_source_url)

    pdap_urls = strip_https_www(pdap_matches_agency['source_url'])
    opd_api_urls = [strip_https_www(y.strip().lower()) for y in df_opd.loc[k, 'api_url_all'].split(',')]
    if len(pdap_matches)==0:
        # Try to match to API URLs
        pdap_matches = pdap_matches_agency[pdap_urls.str.lower().isin(opd_api_urls)]

    # if len(pdap_matches)==0:
    #     pdap_matches, df_opd = find_equivalent_record_type(pdap_matches_agency, df_opd, k)

    if len(pdap_matches)==0:
        df_opd.loc[k, 'new'] = True
        return df_opd
    
    df_opd = compare_values(pdap_matches, df_opd, k)
    df_opd.loc[k, 'airtable_uid'] = ', '.join(pdap_matches['airtable_uid'].tolist())

    return df_opd


def compare_values(pdap_matches, df_opd, k):
    df_opd, is_opd_scraper = check_scraper(pdap_matches, df_opd, k)
    df_opd, bad_url = compare_urls(pdap_matches, df_opd, k)

    if pd.notnull(df_opd.loc[k, 'readme_url']):
        if pdap_matches['readme_url'].isnull().all():
            df_opd.loc[k, 'update_readme'] = 'add'
        else:
            urls = [strip_https_www(x) for x in df_opd.loc[k, 'readme_url'].split(r'\n')]
            matches = [False for _ in range(len(urls))]
            pdap_urls = pdap_matches[pdap_matches['readme_url'].notnull()]['readme_url']
            for pdap_url in strip_https_www(pdap_urls):
                for j,x in enumerate(urls):
                    matches[j] |= pdap_url==x

            if not all(matches):
                df_opd.loc[k, 'update_readme'] = 'update'
                df_opd.loc[k, 'pdap_readme_url'] = '\n'.join(pdap_urls.tolist())
    
    if len(pdap_matches)==1: # If len is not 1 assuming will be manually merged
        if (ds1:=pd.to_datetime(pdap_matches['coverage_start'].iloc[0])) != (ds2:=pd.to_datetime(df_opd.loc[k, 'coverage_start'])) and \
                (pd.notnull(ds1) or pd.notnull(ds2)):
            df_opd.loc[k, 'update_coverage_start'] = ds1 if pd.notnull(ds1) else "NULL"

        if (ds1:=pd.to_datetime(pdap_matches['coverage_end'].iloc[0])) != (ds2:=pd.to_datetime(df_opd.loc[k, 'coverage_end'])) and \
                (pd.notnull(ds1) or (pd.notnull(ds2) and ds2.year!=2024)):  # PDAP appears to use null for data that should be up-to-date
            df_opd.loc[k, 'update_coverage_end'] = ds1 if pd.notnull(ds1) else "NULL"

        if pdap_matches.iloc[0]['access_type'] != df_opd.loc[k, "access_type"]:
            if pd.notnull(pdap_matches.iloc[0]['access_type']):
                if any(x in df_opd.loc[k, "access_type"].split(',') and \
                    x not in pdap_matches.iloc[0]['access_type'] for x in ['API','Download']):
                    df_opd.loc[k, 'update_access_type'] = pdap_matches.iloc[0]['access_type']
                elif all([x in pdap_matches.iloc[0]['access_type'].split(',') for x in df_opd.loc[k, "access_type"].split(',')]):
                    pass
                else:
                    raise NotImplementedError()
            elif df_opd.loc[k, "DataType"]=='Socrata' and extract_socrata_id_from_url(pdap_matches.iloc[0]['source_url']):
                df_opd.loc[k, 'update_access_type'] = True  # Clearly Socrata API
            else:
                raise NotImplementedError()
            
        if pdap_matches.iloc[0]['data_portal_type'] != df_opd.loc[k, "data_portal_type"] and \
            not (pdap_matches['data_portal_type'].isnull().iloc[0] and df_opd.loc[k, 'data_portal_type']==''):
            if bad_url.all():
                df_opd.loc[k, 'update_data_portal_type'] = True
            elif pd.isnull(pdap_matches.iloc[0]['data_portal_type']) and (
                    (df_opd.loc[k, "data_portal_type"]=='Socrata' and extract_socrata_id_from_url(pdap_matches.iloc[0]['source_url'])) or \
                    (df_opd.loc[k, "data_portal_type"]=='ArcGIS' and 'arcgis' in pdap_matches.iloc[0]['source_url'].lower())) :
                df_opd.loc[k, 'update_data_portal_type'] = True  # PDAP URL is definitely from data portal
            elif pd.isnull(pdap_matches.iloc[0]['data_portal_type']) and 'API' in df_opd.loc[k, "access_type"].split(',') and \
                ((m1:=re.search(r'[\w\.]+/',pdap_matches.iloc[0]['source_url'])) and (m2:=re.search(r'[\w\.]+/',df_opd.loc[k, "source_url"])) and \
                re.sub(r'www\d?\.','',m1.group(0))==re.sub(r'www\d?\.','',m2.group(0))):
                df_opd.loc[k, 'update_data_portal_type'] = True  # PDAP URL is from same website as OPD
            elif df_opd.loc[k, "data_portal_type"]=='ArcGIS':
                # Check if website has Arcgis datasets
                m = re.search(r'.+\.(com|gov|org)/',pdap_matches.iloc[0]['source_url'])
                if not m:
                    raise NotImplementedError()
                
                target_url = re.sub(r'https?://','',m.group(0)[:-1])
                ds_url = f'https://hub.arcgis.com/api/feed/all/csv?target={target_url}'
                try:
                    df_ds = pd.read_csv(ds_url)
                    df_opd.loc[k, 'update_data_portal_type'] = True  # PDAP URL is definitely from data portal
                except:
                    raise NotImplementedError()
            elif pdap_matches['data_portal_type'].iloc[0].lower() in df_opd.loc[k,'source_url']:
                pass
            elif pdap_matches.iloc[0]['data_portal_type'] in ['Opendata']:  # Bad values?
                df_opd.loc[k, 'update_data_portal_type'] = pdap_matches.iloc[0]['data_portal_type']
            elif df_opd.loc[k, "SourceName"]=='Fairfax County' and pdap_matches.iloc[0]['data_portal_type'] =='ArcGIS':
                # ArcGIS URL should be bad but not able to test
                df_opd.loc[k, 'update_data_portal_type'] = pdap_matches.iloc[0]['data_portal_type']
            else:
                raise NotImplementedError()

        if len(df_opd.loc[k, "record_format"])>0 and pdap_matches.iloc[0]['record_format'] != df_opd.loc[k, "record_format"]:
            allowable_record_format_changes = [('CSV','XLS')]
            if bad_url.all() or \
                any([y in pdap_matches.iloc[0]['record_format'] and z in df_opd.loc[k, "record_format"].strip() for y,z in allowable_record_format_changes]):
                df_opd.loc[k, 'update_record_format'] = pdap_matches.iloc[0]['record_format']
            elif all(x in pdap_matches.iloc[0]['record_format'].split(',') for x in df_opd.loc[k, "record_format"].split(',')):
                pass
            elif df_opd.loc[k, "SourceName"]=='Fairfax County' and pdap_matches.iloc[0]['data_portal_type'] =='ArcGIS' and \
                pd.notnull(pdap_matches.iloc[0]['record_format']):
                # ArcGIS URL should be bad but not able to test
                df_opd.loc[k, 'update_record_format'] = pdap_matches.iloc[0]['record_format']
            else:
                raise NotImplementedError()
            
        if pdap_matches.iloc[0]['detail_level'] != 'Individual record':
            if pd.isnull(pdap_matches.iloc[0]['detail_level'] ):
                df_opd.loc[k, 'update_detail_level'] = True
            else:
                raise NotImplementedError()

    return df_opd

    

def check_scraper(pdap_matches, df_opd, k):
    
    if not (is_opd_scraper := pdap_matches['scraper_url'].apply(lambda x: pd.notnull(x) and 'openpolicedata' in x.lower()).any()):
        df_opd.loc[k, 'update_opd_scraper'] = True

    return df_opd, is_opd_scraper

def match_source_url(pdap_matches_agency, df_opd, k, opd_source_url):
    pdap_urls = strip_https_www(pdap_matches_agency['source_url'])
    pdap_matches = pdap_matches_agency[pdap_urls==opd_source_url]

    if len(pdap_matches)==0:
        # Try to clean PDAP URL
        pdap_urls = pdap_urls.replace(r'`$', '', regex=True)
        pdap_matches = pdap_matches_agency[pdap_urls==opd_source_url]

    if len(pdap_matches)==0 and df_opd.loc[k, 'DataType']=='Socrata' and (id:=extract_socrata_id_from_url(opd_source_url)) and \
        ((match:=extract_socrata_id_from_url(pdap_matches_agency.loc[:, 'source_url'])==id)).any():
        pdap_matches = pdap_matches_agency[match]

    return pdap_matches, df_opd 

def check_aggregation(state_matches, df_opd, k):
    if any(['Aggregated' in x for x in state_matches]) and \
        any([x in df_opd.loc[k, "AgencyFull"] for x in ['Patrol Division','State Police','State Patrol','Highway Patrol']]):
        df_opd.loc[k, 'update_stanford_not_aggregated'] = [x for x in state_matches if 'Aggregated' in x][0]
    else:
        df_opd.loc[k, 'stanford_new'] = True
    
    return df_opd
    
def compare_urls(pdap_matches, df_opd, k):
    opd_source_url = strip_https_www(df_opd.loc[k,'source_url'])
    is_null = pd.Series(False, pdap_matches.index)
    bad_response = pd.Series(False, pdap_matches.index)
    if len(pdap_matches)>1 or strip_https_www(pdap_matches.iloc[0]['source_url']) != opd_source_url:
        opd_api_urls = [strip_https_www(y.strip()) for y in df_opd.loc[k, 'api_url_all'].split(',')]
        opd_source_urls = [strip_https_www(y.strip()).lower() for y in df_opd.loc[k, 'source_url_all'].split(',')]
    
        is_api_url = pd.Series(False, pdap_matches.index)
        source_equal = pd.Series(False, pdap_matches.index)
        is_source_url = pd.Series(False, pdap_matches.index)
        is_github_url = pd.Series(False, pdap_matches.index)
        for idx in pdap_matches.index:          
            is_null[idx] = pd.isnull(pdap_matches.loc[idx, 'source_url'])
            if is_null[idx]:
                continue

            pdap_url = strip_https_www(pdap_matches.loc[idx, 'source_url'])  
            source_equal[idx] = pdap_url==opd_source_url  
            is_source_url[idx] = pdap_url in opd_source_urls
            is_github_url[idx] = 'github' in pdap_url.lower()

            if not source_equal[idx]:  # Assuming OPD source URls are good, avoids bad responses due to site maintenance, etc.
                bad_response[idx] = not test_url(pdap_matches.loc[idx, 'source_url'], df_opd.loc[k, 'URL'], df_opd.loc[k, 'DataType'])

                pdap_url = strip_https_www(pdap_matches.loc[idx, 'source_url']).lower()  
                is_api_url[idx] = pdap_url in opd_api_urls or pdap_matches.loc[idx, 'source_url'].endswith('.json')

        msg = ''
        for j in pdap_matches.index:
            if is_null[j]:
                msg += r'Null\n'
            elif source_equal[j]:
                msg += r'Match\n'
            elif is_source_url[j]:
                msg += rf'Individual_Source: {pdap_matches.loc[j,'source_url']}\n'
            elif is_api_url[j]:
                msg += rf'API_Source_URL: {pdap_matches.loc[j,'source_url']}\n'
            elif bad_response[j]:
                msg += rf'Dead_URL: {pdap_matches.loc[j,'source_url']}\n'
            else:
                msg += rf'Unmatched: {pdap_matches.loc[j,'source_url']}\n'

        df_opd.loc[k, 'update_source_url'] = msg[:-1]

        if any(['raw.githubusercontent.com/openpolicedata' in x for x in opd_api_urls]):
            if not is_github_url.any():
                df_opd.loc[k, 'OPD Posted Removed Data'] = True
                df_opd.loc[k, "update_agency_originated"] = "newvalue: yes"
                df_opd.loc[k, "update_agency_supplied"] = "newvalue: no"
                df_opd.loc[k, "update_supplying_entity"] = "OpenPoliceData"
            else:
                raise NotImplementedError()
    
    return df_opd, (is_null|bad_response)