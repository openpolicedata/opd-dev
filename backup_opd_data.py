import sys
import os
import zipfile
import numpy as np
import pandas as pd
from geopandas.geodataframe import GeoDataFrame
from shapely import wkt

# Assuming this is either running in the root directory or opd-dev inside the root directory
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","backup")
import openpolicedata as opd
from datetime import datetime

istart = 510
update = None #"changes"
include_stanford = False
src_file = "..\opd-data\opd_source_table.csv"

if src_file != None:
    opd.datasets.datasets = opd.datasets._build(src_file)

datasets = opd.datasets.query()

print(f"Output directory: {output_dir}")
log_filename = f"DataBackup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
log_folder = os.path.join(output_dir,"backup_logs")
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
if not os.path.exists(log_folder):
    os.mkdir(log_folder)

def log_to_file(*args):
    filename = os.path.join(log_folder, log_filename)
    with open(filename, "a") as f:
        for x in args:
            if hasattr(x, '__iter__') and type(x) != str:
                new_line = ', '.join([str(y) for y in x])
            else:
                new_line = str(x)
            
            now = datetime.now().strftime("%d.%b %Y %H:%M")
            f.write(f"{now}: {new_line}\n")

log_to_file("Beginning data backup")

this_year = datetime.now().year

def check_equality(df, df_new, year, is_sorted):
    # is_sorted is helpful for debug breakpoints
    is_equal = True
    for i in range(len(df_new.columns)):
        if "location" in df_new.columns[i].lower() or \
            "latitude" in df_new.columns[i].lower() or \
            "longitude" in df_new.columns[i].lower() or \
            df_new.columns[i] in ['data_loaded_at']:  # date_loaded_at can get updated if they reload
            # Locations can be dicts that can get mangled in CSV conversion
            continue
        a = df.iloc[:,i]
        b = df_new.iloc[:,i]
        if not a.eq(b).all():
            if a.dtype == float:
                if (a-b).abs().max() >= 1e-10:
                    # For debugging
                    diffs = [(a.iloc[k],b.iloc[k]) for k in range(len(df)) if a.iloc[k] != b.iloc[k]]
                    diffs = [x for x in diffs if ((pd.notnull(x[0]) and x[0]!="") or (pd.notnull(x[1]) and x[1]!=""))]
                    if len(diffs)>0:
                        is_equal = False
                        break
            else:
                diffs = [(k,a.iloc[k],b.iloc[k]) for k in range(len(df)) if a.iloc[k] != b.iloc[k]]
                diffs = [x for x in diffs if ((pd.notnull(x[1]) and x[1]!="") or (pd.notnull(x[2]) and x[2]!=""))]
                if (year in [opd.defs.NA, opd.defs.MULTI] or year >= this_year-1) and len(diffs)>0:
                    # This could be something that has been completed like a use of force investigation
                    diffs = [x for x in diffs if (x[1] not in ["NO REPORT"] or x[2] not in ["REPORT"])]
                if len(diffs)>0:
                    # Sometimes numbers are loaded as strings from CSV or vise versa. Try comparing strings
                    diffs = [(k,a.iloc[k],b.iloc[k]) for k in range(len(df)) if str(a.iloc[k]).lower() != str(b.iloc[k]).lower()]
                    diffs = [x for x in diffs if ((pd.notnull(x[1]) and x[1]!="") or (pd.notnull(x[2]) and x[2]!=""))]
                if len(diffs)>0:
                    is_equal = False
                    break

    return is_equal

def compare_dfs(zipname, table, dataset, year):
    df = pd.read_csv(zipname).replace("#N/A",np.nan).replace("NA",np.nan).replace("N/A",np.nan).replace("NULL",np.nan).replace("n/a",np.nan)
    if pd.notnull(dataset["date_field"]):
        df = df.astype({dataset["date_field"]: 'datetime64[ns]'})
    df_new = table.table.replace("#N/A",np.nan).replace("NA",np.nan).replace("N/A",np.nan).replace("NULL",np.nan).replace("n/a",np.nan)
    if type(df_new) == GeoDataFrame:
        df['geometry'] = df['geometry'].apply(wkt.loads)
        df = GeoDataFrame(df, crs=df_new.crs)
        df.pop("geometry")
        df_new.pop("geometry")

    for col in df_new.columns:
        if df_new[col].dtype == "object" and df[col].dtype != "object":
            if df[col].dtype == bool:
                df_new[col] = df_new[col].map({"False" : False, "True" : True, "false" : False, "true" : True, "FALSE" : False, "TRUE" : True}, na_action="ignore")
            else:
                if df[col].dtype==float:
                    df_new[col] = df_new[col].replace("",np.nan)
                elif df[col].dtype == np.int64:
                    df[col] = df[col].astype(pd.Int64Dtype())
                df_new[col] = df_new[col].astype(df[col].dtype)
        elif df_new[col].dtype != "object" and df[col].dtype == "object":
            if df_new[col].dtype==float:
                df[col] = df[col].replace("",np.nan)
            df[col] = df[col].astype(df_new[col].dtype)

    df = df[df_new.columns]

    is_equal = df.eq(df_new).all().all()
    if not is_equal and len(df_new) == len(df):
        is_equal = check_equality(df, df_new, year, False)

        if not is_equal:
            # sortby = []
            # if pd.notnull(dataset["date_field"]) and dataset["date_field"] not in sortby:
            #     sortby = [dataset["date_field"]]
            # sortby.extend(df.columns[0:3])
            df.sort_values(by=list(df.columns), inplace=True, ignore_index=True, key=lambda col: col.apply(lambda x: str(x).lower()))
            df_new.sort_values(by=list(df.columns), inplace=True, ignore_index=True, key=lambda col: col.apply(lambda x: str(x).lower()))
            is_equal = check_equality(df, df_new, year, True)

    if is_equal:
        pass
    elif len(df_new) == len(df):
        pass
    elif (year==opd.defs.NA or year==opd.defs.MULTI or year >= this_year-1) and len(df_new) <= len(df):
        raise ValueError("Data has gotten shorter for recent data")
    elif year!=opd.defs.NA and year!=opd.defs.MULTI and year < this_year-1:
        raise("Older data has changed")

    return is_equal

for i in range(istart, len(datasets)):
    if not include_stanford and "stanford" in datasets.iloc[i]["URL"]:
        continue
    srcName = datasets.iloc[i]["SourceName"]
    state = datasets.iloc[i]["State"]

    table_print = datasets.iloc[i]["TableType"]
    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    print(f"{now} Saving CSV for dataset {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state)

    if datasets.iloc[i]["DataType"] in [opd.defs.DataType.CSV.value, opd.defs.DataType.EXCEL.value] or \
        datasets.iloc[i]["Year"] != opd.defs.MULTI:
        csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], output_dir, datasets.iloc[i]["TableType"])
        zipname = csv_filename.replace(".csv",".zip")

        update_data = False
        if os.path.exists(zipname):
            if update == "changes":
                update_data = True
            else:
                continue

        try:
            table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
        except Exception as e:
            log_to_file(srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], e.args)
            continue

        if update_data and compare_dfs(zipname, table, datasets.iloc[i], datasets.iloc[i]["Year"]):
            continue
            
        csv_filename = table.to_csv(output_dir)
        zipname = csv_filename.replace(".csv",".zip")
        with zipfile.ZipFile(zipname, mode="w", compression=zipfile.ZIP_LZMA) as archive:
            archive.write(csv_filename)
        if not os.path.exists(zipname):
            raise FileExistsError(zipname)
        os.remove(csv_filename)
    else:
        try:
            years = src.get_years(datasets.iloc[i]["TableType"])
        except Exception as e:
            log_to_file(srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], e.args)
            continue

        now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
        print(f"\t{now} Years: {years}")
        
        for year in years:
            csv_filename = src.get_csv_filename(year, output_dir, datasets.iloc[i]["TableType"])

            zipname = csv_filename.replace(".csv",".zip")
            update_data = False
            if os.path.exists(zipname):
                if update == "changes":
                    update_data = True
                else:
                    continue

            print(f"\t{now} Year: {year}")

            try:
                table = src.load_from_url(year, datasets.iloc[i]["TableType"])
            except Exception as e:
                log_to_file(srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], year, e.args)
                continue

            if update_data and compare_dfs(zipname, table, datasets.iloc[i], year):
                continue
                
            csv_filename = table.to_csv(output_dir)
            zipname = csv_filename.replace(".csv",".zip")
            with zipfile.ZipFile(zipname, mode="w", compression=zipfile.ZIP_LZMA) as archive:
                archive.write(csv_filename)
            if not os.path.exists(zipname):
                raise FileExistsError(zipname)
            os.remove(csv_filename)

log_to_file("Completed data backup")