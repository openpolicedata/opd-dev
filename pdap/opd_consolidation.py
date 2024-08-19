# Code for reducing OPD sources table to 1 line per table type per source
# This is to match anticipated PDAP sources where the source_url is assumed to direct the user to all datasets for a given table type

import os
import pandas as pd
import re
import requests
from sodapy import Socrata
import urllib

import openpolicedata as opd

from pdap_utils import strip_https_www, sodapy_key

def reduce(df_opd_orig, opd_consolidated_file):
    if os.path.exists(opd_consolidated_file):
        tmp = pd.read_csv(opd_consolidated_file)
        keep = [x for x in tmp.T.to_dict().values()]
    else:
        keep = []

    # Remove OPD sub-table details
    df_opd_orig['TableType'] = df_opd_orig['TableType'].replace(' - .+$', '', regex=True)
    # Generalize table types to match PDAP
    df_opd_orig['TableType'] = df_opd_orig['TableType'].replace(r'^.+ (STOPS|WARNINGS|CITATIONS)$',r'\1', regex=True)

    df_opd_orig['source_url_type'] = 'orig'
    df_opd_orig['api_url_all'] = df_opd_orig['URL']
    df_opd_orig.loc[df_opd_orig['DataType']=='Socrata', 'api_url_all'] = \
        df_opd_orig.loc[df_opd_orig['DataType']=='Socrata', ['URL','dataset_id']].apply(lambda x: f"{x['URL']}/resource/{x['dataset_id']}.json",axis=1)
    df_opd_orig.loc[df_opd_orig['DataType']=='CKAN', 'api_url_all'] = \
        df_opd_orig.loc[df_opd_orig['DataType']=='CKAN', ['URL','dataset_id']].apply(
            lambda x: f'https://{x["URL"]}/api/3/action/datastore_search_sql?sql=SELECT * FROM "{x['dataset_id']}"',axis=1)

    df_opd_orig['source_url_all'] = df_opd_orig['source_url']

    other_sources = ['stanford', 'muckrock']

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
                 all([(not nullkeep and x in keep[m]['source_url']) + 
                      (not nullcurrent and x in df_opd_orig.loc[k]['source_url'])!=1 for x in other_sources])):
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
            df_opd_orig['URL'].apply(lambda x: not any([y in x.lower() for y in other_sources])) & \
            all([x not in df_opd_orig.loc[k]['URL'] for x in other_sources])
        
        if matches.sum()>1:
            df_matches = df_opd_orig[matches]
            for j in df_matches.index:
                if pd.notnull(df_opd_orig.loc[k]['source_url']):
                    throw = pd.notnull(df_matches.loc[j, 'source_url']) and \
                        strip_https_www(df_opd_orig.loc[k]['source_url'])[:8] != strip_https_www(df_matches.loc[j, 'source_url'])[:8]
                else:
                    throw = True

                if throw and strip_https_www(df_opd_orig.loc[k]['URL'])[:8] != strip_https_www(df_matches.loc[j, 'URL'])[:8] and \
                    df_opd_orig.loc[k]['SourceName'] not in ['Fairfax County']:
                    raise NotImplementedError()
                
            keep[ikeep]['coverage_start'] = df_matches['coverage_start'].min()
            keep[ikeep]['coverage_end'] = df_matches['coverage_end'].max()
            if df_matches['readme'].notnull().any():
                keep[ikeep]['readme'] = r'\n'.join(df_matches['readme'][df_matches['readme'].notnull()].unique().tolist())

            for j in df_matches.index:
                if df_matches.loc[j, 'DataType']=='Socrata':
                    assert df_matches.loc[j,'dataset_id'] in df_matches.loc[j,'source_url']
                    df_matches.loc[j, 'source_url'] = re.search(rf'^.+{df_matches.loc[j,'dataset_id']}', df_matches.loc[j,'source_url']).group(0)

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
                if (df_matches['DataType']=='Socrata').all() and df_matches['URL'].nunique()==1:
                    client = Socrata(df_opd_orig.loc[k]['URL'], sodapy_key)
                    tags = None
                    success = False
                    common_tag_words = None
                    for id in df_matches['dataset_id']:
                        try:
                            meta = client.get_metadata(id)
                            success = True
                        except requests.exceptions.HTTPError:
                            print(f"Unable to access {df_opd_orig.loc[k]['URL']} data for ID {id}")
                            continue
                        
                        if 'tags' in meta:
                            if tags:
                                tags = tags.intersection(meta['tags'])
                            else:
                                tags = set(meta['tags'])

                            cur_words = set()
                            for t in meta['tags']:
                                for w in re.split(r'[\s_-]', t):
                                    w = w.lower()
                                    w = re.sub(r'\d','', w)
                                    if len(w)>0:
                                        cur_words.add(w)

                            if common_tag_words is None:
                                common_tag_words = cur_words
                            else:
                                for w in list(common_tag_words):
                                    if not (w in cur_words or w+'s' in cur_words or w[:-1] in cur_words): # Handle plurals
                                        common_tag_words.discard(w)

                    if not success:
                        del keep[ikeep]
                    elif len(tags)==0 and len(common_tag_words)>0:
                        common_tag_words = list(common_tag_words)
                        # This is the start of code for searching datasets for the least used of the common tags.
                        # However, there is an issue when search all datasets: https://stackoverflow.com/questions/78744494/socrata-find-all-datasets-from-a-domain
                        tag_counts = [0 for _ in common_tag_words]
                        # Retrieve datasets
                        r = requests.get(f'https://api.us.socrata.com/api/catalog/v1?search_context={df_opd_orig.loc[k]['URL']}',
                                        params={'search_context':df_opd_orig.loc[k]['URL']})
                        r.raise_for_status()
                        assert len(r.json()['results'])>0
                        for d in r.json()['results']: # Loop over datasets
                            if d['resource']['id'] in df_matches['dataset_id'].tolist():
                                continue
                            meta = client.get_metadata(d['resource']['id'])

                            if 'tags' in meta:
                                cur_words = set()
                                for t in meta['tags']:
                                    for w in re.split(r'[\s_-]', t):
                                        w = w.lower()
                                        w = re.sub(r'\d','', w)
                                        if len(w)>0:
                                            cur_words.add(w)

                                for m in range(len(common_tag_words)):
                                    if common_tag_words[m] in cur_words:
                                        tag_counts[m]+=1
                                    
                        tag_use = [t for t,c in zip(common_tag_words, tag_counts) if c==min(tag_counts)]
                        keep[ikeep]['source_url'] = f'{df_opd_orig.loc[k]['URL']}/browse?sortBy=relevance&q={tag_use[0]}'
                        keep[ikeep]['source_url_type'] = 'socrata search'
                    else:
                        tags = list(tags)
                        assert len(tags)>0
                        # This is the start of code for searching datasets for the least used of the common tags.
                        # However, there is an issue when search all datasets: https://stackoverflow.com/questions/78744494/socrata-find-all-datasets-from-a-domain
                        tag_counts = [0 for _ in tags]
                        # Retrieve datasets
                        r = requests.get(f'https://api.us.socrata.com/api/catalog/v1?search_context={df_opd_orig.loc[k]['URL']}',
                                        params={'search_context':df_opd_orig.loc[k]['URL']})
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
                        keep[ikeep]['source_url'] = f'{df_opd_orig.loc[k]['URL']}/browse?sortBy=relevance&tags={tag_use[0]}'
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
                        for url in df_matches['URL']:
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
                        cols_check = ['URL','source_url'] if isarcgis else ['source_url']
                        max_word = []
                        for col in cols_check:
                            used = pd.Series(False, index=df_matches.index)
                            words = {}
                            for m,url in df_matches[col].items():
                                if pd.isnull(url):
                                    continue
                                if col=='URL':
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
                            if max(words.values()) / len(df_matches) > 0.5:
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

    return df_opd_red
