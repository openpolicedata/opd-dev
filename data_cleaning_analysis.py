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

istart = 883 #727, 241, 242, 255, 277, 430

csvfile = None
csvfile = r"..\opd-data\opd_source_table.csv"
run_all_stanford = False
run_all_years = True
run_all_agencies = True  # Run all agencies for multi-agency cases
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

if csvfile != None:
    opd.datasets.datasets = opd.datasets._build(csvfile)
datasets = opd.datasets.query()
if run_all_stanford:
    max_num_stanford = float("inf")
else:
    max_num_stanford = 20

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
        except:
            raise
    if len(pkl_filename_in)>0:
        rs = None
        ofs = None
        rofs = None
        new_cols = [x.new_column_name for x in old_data_maps]

        if "SUBJECT_RACE_ONLY" in new_cols:
            rs = "RACE/ETHNICITY"
            for d in old_data_maps:
                if d.new_column_name=="SUBJECT_RACE":
                    d.new_column_name="SUBJECT_RACE/ETHNICITY"
                    d.orig_column_name[0] = "SUBJECT_RACE"
                if d.new_column_name=="SUBJECT_RACE_ONLY":
                    d.new_column_name="SUBJECT_RACE"
        elif "SUBJECT_RACE" in new_cols:
            rs = 'RACE'
            if "SUBJECT_ETHNICITY" not in new_cols:
                idx = [k for k,x in enumerate(new_cols) if x=="SUBJECT_RACE"][0]
                if "HISPANIC / LATINO" in old_data_maps[idx].data_maps.values():
                    rs = "RACE/ETHNICITY"
                    old_data_maps[idx].new_column_name = "SUBJECT_RACE/ETHNICITY"
                    idx = [k for k,x in enumerate(new_cols) if x=="SUBJECT_RE_GROUP"]
                    if idx:
                        old_data_maps[idx[0]].orig_column_name = "SUBJECT_RACE/ETHNICITY"
        elif "OFFICER/SUBJECT_RACE_ONLY" in new_cols:
            rofs = "RACE/ETHNICITY"
            for d in old_data_maps:
                if d.new_column_name=="OFFICER/SUBJECT_RACE":
                    d.new_column_name="OFFICER/SUBJECT_RACE/ETHNICITY"
                    d.orig_column_name[0] = "OFFICER/SUBJECT_RACE"
                if d.new_column_name=="OFFICER/SUBJECT_RACE_ONLY":
                    d.new_column_name="OFFICER/SUBJECT_RACE"
        elif "OFFICER/SUBJECT_RACE" in new_cols:
            rofs = 'RACE'
            if "OFFICER/SUBJECT_ETHNICITY" not in new_cols:
                idx = [k for k,x in enumerate(new_cols) if x=="OFFICER/SUBJECT_RACE"][0]
                if "HISPANIC / LATINO" in old_data_maps[idx].data_maps.values():
                    rofs = "RACE/ETHNICITY"
                    old_data_maps[idx].new_column_name = "OFFICER/SUBJECT_RACE/ETHNICITY"
                    idx = [k for k,x in enumerate(new_cols) if x=="OFFICER/SUBJECT_RE_GROUP"]
                    if idx:
                        old_data_maps[idx[0]].orig_column_name = "OFFICER/SUBJECT_RACE/ETHNICITY"

        if "OFFICER_RACE_ONLY" in new_cols:
            ofs = "RACE/ETHNICITY"
            for d in old_data_maps:
                if d.new_column_name=="OFFICER_RACE":
                    d.new_column_name="OFFICER_RACE/ETHNICITY"
                    d.orig_column_name[0] = "OFFICER_RACE"
                if d.new_column_name=="OFFICER_RACE_ONLY":
                    d.new_column_name="OFFICER_RACE"
        elif "OFFICER_RACE" in new_cols:
            ofs = 'RACE'
            if "OFFICER_ETHNICITY" not in new_cols:
                idx = [k for k,x in enumerate(new_cols) if x=="OFFICER_RACE"][0]
                if "HISPANIC / LATINO" in old_data_maps[idx].data_maps.values():
                    ofs = "RACE/ETHNICITY"
                    old_data_maps[idx].new_column_name = "OFFICER_RACE/ETHNICITY"
                    idx = [k for k,x in enumerate(new_cols) if x=="OFFICER_RE_GROUP"]
                    if len(idx):
                        old_data_maps[idx[0]].orig_column_name = "OFFICER_RACE/ETHNICITY"

        if rs is not None and 'SUBJECT_RE_GROUP' not in new_cols:
            old_data_maps.append(opd._preproc_utils.DataMapping(orig_column_name="SUBJECT_"+rs, new_column_name='SUBJECT_RE_GROUP'))

        if ofs is not None and 'OFFICER_RE_GROUP' not in new_cols:
            old_data_maps.append(opd._preproc_utils.DataMapping(orig_column_name="OFFICER_"+ofs, new_column_name='OFFICER_RE_GROUP'))

        if rofs is not None and 'OFFICER/SUBJECT_RE_GROUP' not in new_cols:
            old_data_maps.append(opd._preproc_utils.DataMapping(orig_column_name="OFFICER/SUBJECT_"+rofs, new_column_name='OFFICER/SUBJECT_RE_GROUP'))

        for j, c in enumerate(new_cols):
            if old_data_maps[j].data_maps is not None and any([isinstance(x,list) for x in old_data_maps[j].data_maps.values()]):
                for k,v in old_data_maps[j].data_maps.items():
                    if isinstance(v, list):
                        v.sort()
                        old_data_maps[j].data_maps[k] = ", ".join(v)
            if "RACE" in c and old_data_maps[j].data_maps:
                for k,v in old_data_maps[j].data_maps.items():
                    # if pd.notnull(k) and isinstance(k,(str,list)) and not (v=="HISPANIC / LATINO" or v=="HISPANIC/LATINO") and \
                    #     (v!="WHITE" and (not isinstance(v,list) or "WHITE" not in v)) and \
                    #         re.findall('whi?te',k,re.IGNORECASE):
                    #     v = [v,'WHITE']
                    #     old_data_maps[j].data_maps[k] = v
                    if " / " in v:
                        v = v.replace(" / ","/")
                        old_data_maps[j].data_maps[k] = v
                    if pd.notnull(k) and isinstance(k,str) and k.lower() in ["f",'filipino'] and v=='ASIAN/PACIFIC ISLANDER':
                        v = "ASIAN"
                        old_data_maps[j].data_maps[k] = v
                    if isinstance(v,list):
                        for m,x in enumerate(v):
                            if " / " in x:
                                v[m] = x.replace(" / ","/")
                                old_data_maps[j].data_maps[k] = v
                        if 'HISPANIC/LATINO' in v:
                            v = 'HISPANIC/LATINO'
                            old_data_maps[j].data_maps[k] = v

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
                            for key in old_data_maps[j].data_maps.keys():
                                key2 = key
                                if pd.isnull(key) or key=='NA':
                                    key2 = [x for x in data_maps[k].data_maps.keys() if pd.isnull(x)][0]
                                if old_data_maps[j].data_maps[key]!=data_maps[k].data_maps[key2]:
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
                    logger.info("Unmatched old column: ")
                    logger.info(old_data_maps[j])
                    diff_found = True

            for k in range(len(data_maps)):
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
    except opd.exceptions.OPD_DataUnavailableError:
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
            
            if os.path.exists(zip_filename) or os.path.exists(csv_filename):
                is_zip = os.path.exists(zip_filename)
                try:
                    table = src.load_from_csv(y, table_type=datasets.iloc[i]["TableType"], 
                        agency=agency,output_dir=backup_dir, zip=is_zip)
                except pd.errors.EmptyDataError:
                    continue
                except:
                    raise
            elif run_all_years:
                try:
                    table = src.load_from_url(y, table_type=datasets.iloc[i]["TableType"], 
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

            # col_map = {}
            # for x in table.get_transform_map():
            #     k = ",".join(x.orig_column_name) if isinstance(x.orig_column_name,list) else x.orig_column_name
            #     col_map[k] = x.new_column_name

            # # Check if data in map
            # if datasets.iloc[i]["State"] not in std_map:
            #     std_map[datasets.iloc[i]["State"]] = {}
            # if datasets.iloc[i]["SourceName"] not in std_map[datasets.iloc[i]["State"]]:
            #     std_map[datasets.iloc[i]["State"]][datasets.iloc[i]["SourceName"]] = {}
            # if datasets.iloc[i]["TableType"] in std_map[datasets.iloc[i]["State"]][datasets.iloc[i]["SourceName"]]:
            #     raise NotImplementedError("Need to implement")
            # else:
            #     is_accepted = False
            #     if is_accepted:
            #         std_map[datasets.iloc[i]["State"]][datasets.iloc[i]["SourceName"]][datasets.iloc[i]["TableType"]] = []
                    
            #         std_map[datasets.iloc[i]["State"]][datasets.iloc[i]["SourceName"]][datasets.iloc[i]["TableType"]]

            if table.is_std:
                log_to_json(orig_columns, table.get_transform_map(), csv_filename, 
                            datasets.iloc[i]["SourceName"], datasets.iloc[i]["TableType"], y)
                    
            if not run_all_years:
                break
    else:
        csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency)
        zip_filename = csv_filename.replace(".csv",".zip")
        if os.path.exists(csv_filename) or os.path.exists(zip_filename):
            is_zip = os.path.exists(zip_filename)
            table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                agency=agency,output_dir=backup_dir, zip=is_zip)
        else:
            try:
                table = src.load_from_url(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
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
    

logger.info("Complete")
