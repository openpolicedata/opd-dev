import copy
import re
import os
import pickle
import sys
from datetime import date
import logging
from glob import glob
import pandas as pd
from datetime import datetime
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","backup")
import openpolicedata as opd

backup_dir = output_dir
output_dir = os.path.join(output_dir, 'standardization')

if not os.path.exists(output_dir):
    raise FileNotFoundError(f"Output directory {output_dir} does not exist")

# pd.to_datetime(x, errors='ignore')
#601: Sparks?
# FutureWarning: 'A-DEC' is deprecated and will be removed in a future version, please use 'Y-DEC' instead.
# 605: NJ State Police
# C:\Users\matth\repos\openpolicedata\..\openpolicedata\openpolicedata\datetime_parser.py:22: FutureWarning: Setting an item of incompatible dtype is deprecated and will raise in a future error of pandas. Value '0         2020
# Name: year, Length: 398929, dtype: int64' has dtype incompatible with period[Y-DEC], please explicitly cast to a compatible dtype first.
#   d.iloc[:,0] = date_col.iloc[:,0].dt.year
# Tucson OIS
#  FutureWarning: In a future version of pandas, parsing datetimes with mixed time zones will raise an error unless `utc=True`. Please specify `utc=True` to opt in to the new behaviour and silence this warning. To create a `Series` with mixed offsets and `object` dtype, please use `apply` and `datetime.datetime.strptime`
istart = 745

csvfile = None
csvfile = r"..\opd-data\opd_source_table.csv"
run_all_stanford = False
run_all_years = True
run_all_agencies = True  # Run all agencies for multi-agency cases
force_load_from_url = False
load_if_date_before = '01/28/2024'
verbose = False
allowed_updates = []

skip_sources = []
logger = logging.getLogger("opd")
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

if csvfile:
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

prev_columns = []
prev_maps = {}
def log_to_json(orig_columns, data_maps, csv_filename, source_name, table_type, year):
    pkl_filename_wc = os.path.basename(csv_filename).replace(".csv","*.pkl")
    pkl_filename_in = glob(os.path.join(output_dir, pkl_filename_wc))
    pkl_filename_out = os.path.join(output_dir, 
                 os.path.basename(csv_filename).replace(".csv","") + f"_{datetime.now().strftime('%Y%m%d')}_pd{pd.__version__.replace('.','_')}.pkl")
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
                        elif (m:=re.search(r"^(.+)_RACE(/ETHNICITY)?$", old_data_maps[j].orig_column_name)) and \
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
                if data_maps[k].new_column_name=='ZIP_CODE':
                    # Ignoring since new
                    continue
                if idx_new[k]<0:
                    logger.info("Unmatched new column: ")
                    logger.info(data_maps[k])
                    diff_found = True

            raise_error = True
            if diff_found and raise_error:
                raise ValueError(f"Check {pkl_filename_in[0]}!")
            elif not diff_found:
                [os.remove(x) for x in pkl_filename_in]
                pickle.dump(data_maps, open(pkl_filename_out, "wb"))
                return
    
    logger.debug(f"Original columns:\n{orig_columns}")
    
    # Skip if shown before
    same_table = prev_columns[0]==source_name and prev_columns[1]==table_type if len(prev_columns)>0 else False
    all_orig = [map.orig_column_name for map in data_maps]
    all_new = [map.new_column_name for map in data_maps]
    if not same_table or \
        all_orig!=prev_columns[2] or all_new!=prev_columns[3]:
        msg = "Identified columns:\n"
        for map in data_maps:
            msg+=f"\t{map.orig_column_name}: {map.new_column_name}\n"

        [prev_columns.pop() for _ in range(len(prev_columns))]
        prev_columns.append(source_name)
        prev_columns.append(table_type)
        prev_columns.append(all_orig)
        prev_columns.append(all_new)
        prev_columns.append(dict())
    
        logger.debug(msg)
    else:
        logger.debug("Same column mapping as previously run case\n")

    msg = "Data Maps:\n"
    for map in data_maps:
        # if same_table and map.data_maps is not None:
        if map.data_maps is not None:
            if map.orig_column_name not in prev_columns[-1]:
                prev_columns[-1][map.orig_column_name] = dict()
            map_copy = copy.deepcopy(map)
            for k,v in map.data_maps.items():
                if k in prev_columns[-1][map.orig_column_name]:
                    if prev_columns[-1][map.orig_column_name][k] != v:
                        raise ValueError(f"Value of {k} in column {map.orig_column_name} expected to map to {prev_columns[-1][map.orig_column_name][k]} but actually maps to {v}")
                    map_copy.data_maps.pop(k)
                else:
                    prev_columns[-1][map.orig_column_name][k] = v
            map = map_copy
                    
        msg+=f"{map}\n\n"

    logger.debug(msg)

    logger.debug("----------------------------------------------------------------------------")
    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug("----------------------------------------------------------------------------")

    [os.remove(x) for x in pkl_filename_in]
    pickle.dump(data_maps, open(pkl_filename_out, "wb"))


num_stanford = 0
prev_sources = []
prev_tables = []
prev_states = []
move_to_end = []
has_issue = datasets["SourceName"].apply(lambda x : x in move_to_end)
no_issue = datasets["SourceName"].apply(lambda x : x not in move_to_end)
datasets = pd.concat([datasets[no_issue], datasets[has_issue]])
for i in range(istart, len(datasets)):
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

    skip = False
    for k in range(len(prev_sources)):
        if srcName == prev_sources[k] and datasets.iloc[i]["TableType"] ==prev_tables[k] and \
            datasets.iloc[i]["State"] == prev_states[k]:
            skip = True

    if skip or any([x==srcName and (y==datasets.iloc[i]["TableType"] or y=="ALL") and z=="ALL" for x,y,z in skip_sources]):
        continue

    prev_sources.append(srcName)
    prev_tables.append(datasets.iloc[i]["TableType"])
    prev_states.append(datasets.iloc[i]["State"])

    table_print = datasets.iloc[i]["TableType"]
    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    logger.info(f"{now} Running index {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state)

    year = date.today().year
    table = None
    csv_filename = "NOT A FILE"
    try:
        years = src.get_years(datasets.iloc[i]["TableType"])
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

    if ".zip" in datasets.iloc[i]["URL"] and datasets.iloc[i]["Year"]==opd.defs.MULTI:
        load_by_year = False

    if load_by_year:
        for y in years:
            if any([x==srcName and (t==datasets.iloc[i]["TableType"] or t=="ALL") and (z==y or z=="ALL") for x,t,z in skip_sources]):
                logger.info(f"Skipping year {y}")
                continue
            logger.info(f"Year: {y}")
            try:
                csv_filename = src.get_csv_filename(y, backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency)
                zip_filename = csv_filename.replace(".csv",".zip")
            except ValueError as e:
                if "There are no sources matching tableType" in e.args[0]:
                    continue
                else:
                    raise
            except:
                raise
            
            if load_csv(force_load_from_url, load_if_date_before, csv_filename, zip_filename):
                is_zip = not os.path.exists(csv_filename)
                try:
                    table = src.load_from_csv(y, table_type=datasets.iloc[i]["TableType"], 
                        agency=agency,output_dir=backup_dir, zip=is_zip)
                except pd.errors.EmptyDataError:
                    continue
                except:
                    raise

                if len(table.table)==0:
                    table = src.load(datasets.iloc[i]["TableType"], y,
                            agency=agency)
                table.to_csv(output_dir=backup_dir)
            elif run_all_years:
                try:
                    table = src.load(datasets.iloc[i]["TableType"], y,
                            agency=agency)
                except opd.exceptions.OPD_FutureError:
                    continue
                except:
                    raise
                if len(table.table)==0:
                    continue
                table.to_csv(output_dir=backup_dir)
            else:
                continue
            
            orig_columns = table.table.columns
            table.standardize(race_cats= "expand", agg_race_cat=True, verbose=verbose, no_id="test")

            log_to_json(orig_columns, table.get_transform_map(), csv_filename, 
                        datasets.iloc[i]["SourceName"], datasets.iloc[i]["TableType"], y)
            
            a = copy.deepcopy(table)
            table.expand(mismatch='splitsingle')
            a.expand(mismatch='nan')
            related_table, related_years = src.find_related_tables(table.table_type, table.year)

            for rt, ry in zip(related_table, related_years):
                t2 = src.load(rt, ry)
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
                    agency=agency)
        zip_filename = csv_filename.replace(".csv",".zip")
        if load_csv(force_load_from_url, load_if_date_before, csv_filename, zip_filename):
            is_zip = not os.path.exists(csv_filename)
            table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                agency=agency,output_dir=backup_dir, zip=is_zip)
        else:
            try:
                table = src.load(datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], 
                        agency=agency)
            except opd.exceptions.OPD_FutureError:
                continue
            except:
                raise
            table.to_csv(output_dir=backup_dir)

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
