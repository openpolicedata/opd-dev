import copy
import re
import os
import pickle
import random
from rapidfuzz import fuzz
import sys
from datetime import date
import logging
from glob import glob
import pandas as pd
import warnings
from datetime import datetime
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    sys.path.append(os.path.join("..","openpolicedata",'tests'))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    sys.path.append(os.path.join("..",'tests'))
    output_dir = os.path.join("..","data","backup")
    
import openpolicedata as opd
import test_utils

backup_dir = output_dir
output_dir = os.path.join(output_dir, 'standardization')

if not os.path.exists(output_dir):
    raise FileNotFoundError(f"Output directory {output_dir} does not exist")

istart = 1325

use_changed_rows = False
csvfile = None
csvfile = r"..\opd-data\opd_source_table.csv"
exclude_url = ['data-openjustice','stanford']
min_year = 2000
run_all_stanford = False
run_all_years = True
run_all_agencies = True  # Run all agencies for multi-agency cases
force_load_from_url = False
table_to_run = None
load_if_date_before = '01/28/2024'
verbose = False
allowed_updates = []
perc_update = 0.2

if csvfile and not os.path.exists(csvfile):
    csvfile = os.path.join('..',csvfile)
if csvfile:
    assert os.path.exists(csvfile)

skip_sources = []
logger = logging.getLogger("opd_clean")
sh = logging.StreamHandler()
fh = logging.FileHandler(os.path.join(output_dir, f'std_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'))
format = logging.Formatter("%(asctime)s :: %(message)s", '%y-%m-%d %H:%M:%S') 
sh.setFormatter(format)
fh.setFormatter(format)
fh.setLevel(logging.DEBUG)
sh.setLevel(logging.INFO)

logger.addHandler(fh)
logger.addHandler(sh)
logger.setLevel(logging.DEBUG)

if use_changed_rows:
    changed_datasets = test_utils.get_datasets(csvfile, use_changed_rows)
    if not csvfile:
        raise ValueError("CSV file currently must be set")
    opd.datasets.reload(csvfile)
elif csvfile:
    opd.datasets.reload(csvfile)
datasets = opd.datasets.query()
if run_all_stanford:
    max_num_stanford = float("inf")
else:
    max_num_stanford = 20

def load_csv(force_load_from_url, load_if_date_before, csv_filename, zip_filename):
    return not force_load_from_url and\
        (os.path.exists(zip_filename) or os.path.exists(csv_filename)) and \
        (not load_if_date_before or \
            (os.path.exists(csv_filename) and \
             pd.to_datetime(os.path.getmtime(csv_filename), unit='s', origin='unix')>=pd.to_datetime(load_if_date_before)))

prev_columns = {}
prev_maps = {}
def log_to_json(orig_columns, data_maps, csv_filename, source_name, table_type, year):
    pkl_filename_wc = os.path.basename(csv_filename).replace(".csv","*.pkl")
    pkl_filename_in = glob(os.path.join(output_dir, pkl_filename_wc))
    pkl_filename_out = os.path.join(output_dir, 
                 os.path.basename(csv_filename).replace(".csv","") + f"_{datetime.now().strftime('%Y%m%d')}_pd{pd.__version__.replace('.','_')}.pkl")
    is_last_year = False
    if len(pkl_filename_in)==0 and csv_filename.endswith(f"_{datetime.now().year}.csv"):
        # Compare to last year
        pkl_filename_wc = os.path.basename(csv_filename).replace(".csv","*.pkl").replace(f"_{datetime.now().year}", f"_{datetime.now().year-1}")
        pkl_filename_in = glob(os.path.join(output_dir, pkl_filename_wc))
        is_last_year = len(pkl_filename_in)>0

    if len(pkl_filename_in)>1:
        for p in pkl_filename_in[:-1]:
            os.remove(p)
        pkl_filename_in = [pkl_filename_in[-1]]
    if len(pkl_filename_in)>0:
        try:
            old_data_maps = pickle.load(open(pkl_filename_in[0], "rb"))
        except ModuleNotFoundError as e:
            if "_pd" in pkl_filename_in[0]:
                os.remove(pkl_filename_in[0])
                pkl_filename_in = []
                # raise NotImplementedError("pd version access")
            else:
                os.remove(pkl_filename_in[0])
                pkl_filename_in = []
        except ValueError as e:
            if str(e)=='Invalid frequency: Y-DEC':
                os.remove(pkl_filename_in[0])
                pkl_filename_in = []
            else:
                raise
        except Exception as e:
            raise
    if len(pkl_filename_in)>0:
        if data_maps==old_data_maps:
            # Update to deal with pandas back-compatibility issue
            if pkl_filename_out != pkl_filename_in[0]:
                [os.remove(x) for x in pkl_filename_in]
                pickle.dump(data_maps, open(pkl_filename_out, "wb"))
            return
        elif any([x[0]==source_name and (len(x)<2 or x[1]==table_type) and (len(x)<3 or year in x[2]) for x in allowed_updates]):
            pass
        else:
            diff_found = False
            idx_old = [-1 for _ in old_data_maps]
            matches_old = [False for _ in old_data_maps]
            idx_new = [-1 for _ in data_maps]
            matches_new = [False for _ in data_maps]
            for j in range(len(old_data_maps)):
                for k in range(len(data_maps)):
                    if idx_new[k]>=0:
                        continue
                    if old_data_maps[j]==data_maps[k]:
                        idx_old[j] = k
                        idx_new[k] = j
                        matches_old[j] = matches_new[k] = True
                        break
                    elif old_data_maps[j].new_column_name == data_maps[k].new_column_name:
                        # if old_data_maps[j].orig_value_counts is not None:
                        #     old_data_maps[j].orig_value_counts.index = old_data_maps[j].orig_value_counts.index.astype(data_maps[k].orig_value_counts.index.dtype)
                        if old_data_maps[j].orig_column_name == data_maps[k].orig_column_name:
                            if old_data_maps[j].data_maps:
                                for key in old_data_maps[j].data_maps.keys():
                                    key2 = key
                                    if pd.isnull(key) or key=='NA':
                                        key2 = [x for x in data_maps[k].data_maps.keys() if pd.isnull(x) or (isinstance(x,str) and x in 'N/A')]
                                        if len(key2)==0:
                                            if 'None' in data_maps[k].data_maps.keys() and \
                                                'None' not in old_data_maps[j].data_maps.keys():
                                                continue
                                            else:
                                                continue

                                        key2 = key2[0]
                                    if key2 in data_maps[k].data_maps and old_data_maps[j].data_maps[key]!=data_maps[k].data_maps[key2]:
                                        if isinstance(old_data_maps[j].data_maps[key], list) and \
                                            isinstance(data_maps[k].data_maps[key2], list) and \
                                            len(old_data_maps[j].data_maps[key])==len(data_maps[k].data_maps[key2]) and \
                                            set(old_data_maps[j].data_maps[key])==set(data_maps[k].data_maps[key2]):
                                            continue
                                        break
                                else:
                                    idx_old[j] = k
                                    idx_new[k] = j
                                    matches_old[j] = matches_new[k] = True
                                    break
                            else:
                                idx_old[j] = k
                                idx_new[k] = j
                                matches_old[j] = matches_new[k] = True
                                break
                        elif isinstance(old_data_maps[j].orig_column_name,str) and \
                            (m:=re.search(r"^(.+)_RACE(/ETHNICITY)?$", old_data_maps[j].orig_column_name)) and \
                            re.search(rf"^{m.group(1)}_RACE(/ETHNICITY)?$", data_maps[k].orig_column_name):
                            idx_old[j] = k
                            idx_new[k] = j
                            matches_old[j] = matches_new[k] = True
                            break
                        idx_old[j] = k
                        idx_new[k] = j
                        matches_old[j] = matches_new[k] = False
                        logger.info("Unequal column data")
                        logger.info("Old data map")
                        logger.info(old_data_maps[j])
                        logger.info("\nNew data map: ")
                        logger.info(data_maps[k])
                        diff_found = True
                        break
                else:
                    if (m:=re.search(r"^(.+)_RACE(/ETHNICITY)?$", old_data_maps[j].new_column_name)) and \
                        any((m:=[re.search(rf"^{m.group(1)}_RACE(/ETHNICITY)?$", x.new_column_name) for x in data_maps])):
                        k = [n for n,x in enumerate(m) if x][0]
                        idx_old[j] = k
                        idx_new[k] = j
                        matches_old[j] = matches_new[k] = True
                        continue
                    logger.info("Unmatched old column: ")
                    logger.info(old_data_maps[j])
                    diff_found = True

            for k in range(len(data_maps)):
                if idx_new[k]<0:
                    logger.info("Unmatched new column: ")
                    logger.info(data_maps[k])
                    diff_found = True

            if diff_found and is_last_year:
                logger.info('****COMPARING TO LAST YEAR*****')

            raise_error = True
            if 'CRASHES' in table_type:
                warnings.warn('Skipping crashes tables because of demograhics std change')
                raise_error = False
            if diff_found and raise_error:
                raise ValueError(f"Check {pkl_filename_in[0]}!")
            elif not diff_found:
                if not is_last_year:
                    [os.remove(x) for x in pkl_filename_in]
                pickle.dump(data_maps, open(pkl_filename_out, "wb"))
                return
    
    logger.debug(f"Original columns:\n{orig_columns}")
    
    # Skip if shown before
    same_table = prev_columns['source']==source_name and prev_columns['table']==table_type if len(prev_columns)>0 else False
    all_orig = [map.orig_column_name for map in data_maps]
    all_new = [map.new_column_name for map in data_maps]
    if not same_table or \
        all_orig!=prev_columns['orig_cols'] or all_new!=prev_columns['new_cols']:
        msg = "Identified columns:\n"
        for map in data_maps:
            msg+=f"\t{map.orig_column_name}: {map.new_column_name}\n"
        
        logger.debug(msg)

        prev_columns['source'] =source_name
        prev_columns['table'] = table_type
        prev_columns['orig_cols'] = all_orig
        prev_columns['new_cols'] = all_new
        prev_columns['data'] = dict()
    else:
        logger.debug("Same column mapping as previously run case\n")

    msg = "Data Maps:\n"
    for map in data_maps:
        # if same_table and map.data_maps is not None:
        if map.data_maps is not None:
            key = str(map.orig_column_name)
            if key not in prev_columns['data']:
                prev_columns['data'][key] = dict()
            map_copy = copy.deepcopy(map)
            for k,v in map.data_maps.items():
                if k in prev_columns['data'][key]:
                    if prev_columns['data'][key][k] != v:
                        raise ValueError(f"Value of {k} in column {map.orig_column_name} expected to map to {prev_columns['data'][key][k]} but actually maps to {v}")
                    map_copy.data_maps.pop(k)
                else:
                    prev_columns['data'][key][k] = v
            map = map_copy
                    
        msg+=f"{map}\n\n"

    logger.debug(msg)

    logger.debug("----------------------------------------------------------------------------")
    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug("----------------------------------------------------------------------------")

    [os.remove(x) for x in pkl_filename_in]
    pickle.dump(data_maps, open(pkl_filename_out, "wb"))


num_stanford = 0
for i in range(istart, len(datasets)):
    if use_changed_rows and not changed_datasets.apply(lambda x: pd.Series(x.to_dict()).equals(pd.Series(datasets.iloc[i].to_dict())), axis=1).any():
        continue

    if any(x in datasets.iloc[i]["URL"] for x in exclude_url):
        continue

    if "stanford.edu" in datasets.iloc[i]["URL"]:
        num_stanford += 1
        if num_stanford > max_num_stanford or datasets.iloc[i]["SourceName"]==datasets.iloc[i]["State"] or \
            datasets.iloc[i]["SourceName"]=="State Patrol":
            continue

    if datasets.iloc[i]["TableType"].lower() in opd.preproc._skip_tables:
        continue

    srcName = datasets.iloc[i]["SourceName"]
    state = datasets.iloc[i]["State"]

    if not run_all_agencies and datasets.iloc[i]["Agency"] == opd.defs.MULTI and srcName == "Virginia":
        # Reduce size of data load by filtering by agency
        agency = "Fairfax County Police Department"
    else:
        agency = None


    if any([x==srcName and (y==datasets.iloc[i]["TableType"] or y=="ALL") and z=="ALL" for x,y,z in skip_sources]):
        continue

    table_print = datasets.iloc[i]["TableType"]

    if table_to_run and datasets.iloc[i]["TableType"] not in table_to_run:
        logger.info(f"Not in tables to run. Skipping index {i} of {len(datasets)}: {srcName} {table_print} table")
        continue

    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    logger.info(f"{now} Running index {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

    year = date.today().year
    table = None
    csv_filename = "NOT A FILE"
    try:
        years = src.get_years(datasets.iloc[i]["TableType"], datasets=datasets.iloc[i])
        years.sort(reverse=True)
        load_by_year = True
    except (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError):
        csv_filename = src.get_csv_filename(2023, backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency)
        csv_filename = csv_filename.replace("2023","*").replace(".csv",".*")
        all_files = glob(csv_filename)
        years = [int(re.findall(r"_(\d+).(csv|zip)", x)[0][0]) for x in all_files]
        years.sort(reverse=True)
        load_by_year = True
    except opd.exceptions.OPD_FutureError:
        continue
    except Exception as e:
        if datasets.iloc[i]["DataType"] not in ["CSV", "Excel"]:
            raise
        load_by_year = False

    if (".zip" in datasets.iloc[i]["URL"] or pd.isnull(datasets.iloc[i]["date_field"])) and datasets.iloc[i]["Year"]==opd.defs.MULTI:
        load_by_year = False

    csv_add_on = False
    if  datasets.iloc[i]["Year"]==opd.defs.MULTI:
        ds_filtered, _ = src._Source__filter_for_source(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], None, None, errors=False)
        if isinstance(ds_filtered, pd.DataFrame):
            not_match = ds_filtered.apply(lambda x: not x.equals(datasets.iloc[i]), axis=1)
            unique_url = []
            sim = []
            for k in not_match[not_match].index:
                unurl = ''
                sim.append(fuzz.partial_ratio(datasets.iloc[i]['URL'], ds_filtered.loc[k,'URL']))
                for j in range(len(datasets.iloc[i]['URL'])):
                    if j < len(ds_filtered.loc[k,'URL']):
                        if ds_filtered.loc[k,'URL'][j]!=datasets.iloc[i]['URL'][j]:
                            unurl += datasets.iloc[i]['URL'][j]
                    else:
                        unurl += datasets.iloc[i]['URL'][j:]
                        break
                
                if len(unurl)==0:
                    assert pd.notnull(datasets.iloc[i]['dataset_id'])
                    # These differ by dataset ID
                    unurl = datasets.iloc[i]['dataset_id'].split('.')[0]
                    
                unique_url.append(unurl)
            
            unique_url = [x for x,y in zip(unique_url, sim) if y==max(sim)]
            csv_add_on = unique_url[0].replace('/','_')
            csv_add_on = csv_add_on[:min(len(csv_add_on),10)]

    if load_by_year:
        years = [y for y in years if y>=min_year]
        for y in years:
            if any([x==srcName and (t==datasets.iloc[i]["TableType"] or t=="ALL") and (z==y or z=="ALL") for x,t,z in skip_sources]):
                logger.info(f"Skipping year {y}")
                continue
            logger.info(f"Year: {y}")
            try:
                csv_filename = src.get_csv_filename(y, backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
            except ValueError as e:
                if "There are no sources matching tableType" in e.args[0]:
                    continue
                else:
                    raise
            except:
                raise

            if csv_add_on:
                csv_filename = csv_filename.replace(".csv","_"+csv_add_on+".csv")
            zip_filename = csv_filename.replace(".csv",".zip")
            
            load_new = random.random() < perc_update if datasets.iloc[i]["TableType"]!='TRAFFIC STOPS' or datasets.iloc[i]["SourceName"]!='St. Paul' else True
            if not load_new and load_csv(force_load_from_url, load_if_date_before, csv_filename, zip_filename):
                is_zip = not os.path.exists(csv_filename)
                try:
                    table = src.load_from_csv(y, table_type=datasets.iloc[i]["TableType"], 
                        agency=agency,output_dir=backup_dir, zip=is_zip, url=datasets.iloc[i]['URL'], 
                        id=datasets.iloc[i]['dataset_id'], 
                        filename=os.path.basename(csv_filename))
                except pd.errors.EmptyDataError:
                    continue
                except:
                    raise

                if len(table.table)==0:
                    table = src.load(datasets.iloc[i]["TableType"], y,
                            agency=agency)
                    table.to_csv(output_dir=backup_dir, filename=os.path.basename(csv_filename))
            elif run_all_years:
                try:
                    table = src.load(datasets.iloc[i]["TableType"], y,
                            agency=agency,  url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
                except (opd.exceptions.OPD_FutureError, opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError):
                    continue
                except:
                    raise
                if len(table.table)==0:
                    continue
                table.to_csv(output_dir=backup_dir, filename=os.path.basename(csv_filename))
            else:
                continue
            
            orig_columns = table.table.columns
            table.standardize(race_cats= "expand", agg_race_cat=True, verbose=verbose, no_id="test")

            log_to_json(orig_columns, table.get_transform_map(), csv_filename, 
                        datasets.iloc[i]["SourceName"], datasets.iloc[i]["TableType"], y)
            
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore",category=DeprecationWarning, message='Passing a BlockManager')
                a = copy.deepcopy(table)
            table.expand(mismatch='splitsingle')
            a.expand(mismatch='nan')
            related_table, related_years = src.find_related_tables(table.table_type, table.year)

            for rt, ry in zip(related_table, related_years):
                try:
                    related_ds = src.datasets[(src.datasets['TableType']==rt) & (src.datasets['Year']==ry)]
                    if len(related_ds)==1 and pd.notnull(related_ds.iloc[0]['date_field']) and y!="NONE":
                        t2 = src.load(rt, y)
                    else:
                        t2 = src.load(rt, ry)
                except (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError):
                    continue
                t2.standardize(race_cats= "expand", agg_race_cat=True, verbose=verbose, no_id="test")
                try:
                    # Merge incident and subjects tables on their unique ID columns to create 1 row per subject
                    table2 = table.merge(t2, std_id=True)
                except opd.exceptions.AutoMergeError as e:
                    if any([y in table.table.columns for y in ['tax_id','complaint_id','randomized_officer_id','Latitude','Longitude']]) and \
                        any([y in t2.table.columns for y in ['tax_id','complaint_id','randomized_officer_id','Latitude','Longitude']]):
                        continue
                    else:
                        raise
                except:
                    raise
                    
            if not run_all_years:
                break
    else:
        csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
        if csv_add_on:
            csv_filename = csv_filename.replace(".csv","_"+csv_add_on+".csv")
        zip_filename = csv_filename.replace(".csv",".zip")
        load_new = random.random() < perc_update
        if not load_new and load_csv(force_load_from_url, load_if_date_before, csv_filename, zip_filename):
            is_zip = not os.path.exists(csv_filename)
            table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                agency=agency,output_dir=backup_dir, zip=is_zip, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
        else:
            try:
                table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], 
                        agency=agency, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
            except opd.exceptions.OPD_FutureError:
                continue
            except:
                raise
            table.to_csv(output_dir=backup_dir, filename=os.path.basename(csv_filename))

        orig_columns = table.table.columns
        table.standardize(race_cats= "expand", agg_race_cat=True, verbose=verbose, no_id="test")

        log_to_json(orig_columns, table.get_transform_map(), csv_filename, 
                    datasets.iloc[i]["SourceName"], datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"])
        
        a = copy.deepcopy(table)
        a.expand(mismatch='nan')
        table.expand(mismatch='splitsingle')
        related_table, related_years = src.find_related_tables(table.table_type, table.year)
        for rt, ry in zip(related_table, related_years):
            t2 = src.load(rt, ry)
            t2.standardize(race_cats= "expand", agg_race_cat=True, verbose=verbose, no_id="test")
            try:
                # Merge incident and subjects tables on their unique ID columns to create 1 row per subject
                table2 = table.merge(t2, std_id=True)
            except ValueError as e:
                if len(e.args)>0 and e.args[0]=='No incident ID column found' and \
                    src["SourceName"]=='Charlotte-Mecklenburg':
                    # Dataset has no incident ID column. Latitude/longitude seems to work instead
                    table2 = table.merge(t2, on=['Latitude','Longitude'])
                else:
                    raise
            except:
                raise
    

logger.info("Complete")
