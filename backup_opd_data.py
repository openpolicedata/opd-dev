import numbers
import pandas as pd
import os
import random
import sys
from datetime import datetime
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    sys.path.append(os.path.join("..","openpolicedata",'tests'))
    backup_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    sys.path.append(os.path.join("..",'tests'))
    backup_dir = os.path.join("..","data","backup")
    
import openpolicedata as opd

istart = 0

csvfile = None
csvfile = r"..\opd-data\opd_source_table.csv"
run_single_year_per_dataset = True
perc_update = 0.2

if csvfile and not os.path.exists(csvfile):
    csvfile = os.path.join('..',csvfile)
if csvfile:
    assert os.path.exists(csvfile)

if csvfile:
    opd.datasets.reload(csvfile)
datasets = opd.datasets.query()

def clean(x):
    if pd.isnull(x):
        x = ''
    elif isinstance(x,str):
        if x.lower()=='false':
            x = False
        elif x.lower()=='true':
            x = True

        try:
            x = float(x)
        except:
            pass
    if isinstance(x, numbers.Number):
        x = round(x, 6)
    x = str(x).strip().lstrip('0').rstrip('.0')
    if x.lower() in ['none','nan']:
        x = ''
    return x.lower()

def comp(df, df1, csv_name):
    if len(df1) == len(df):
        if len(df1.columns)>len(df.columns):
            return
        elif len(df1.columns)<len(df.columns):
            raise NotImplementedError()
        
        if df.equals(df1) or \
            ((df==df1) | (df.isnull() & df1.isnull()) | \
                (df.apply(lambda x: x.apply(clean))==df1.apply(lambda x: x.apply(clean)))).all().all():
            os.remove(csv_name)
        else:
            cols = df.equals(df1) or \
                ((df==df1) | (df.isnull() & df1.isnull()) | \
                    (df.apply(lambda x: x.apply(clean))==df1.apply(lambda x: x.apply(clean)))).all()
            col = cols[~cols].index[0]
            tf=df[col].apply(clean)!=df1[col].apply(clean)
            print(df[col][tf].iloc[0])
            print(df1[col][tf].iloc[0])
            raise NotImplementedError()
    elif len(df1)<len(df):
        assert not ((df.isnull().mean()>0.1) & (df1.isnull().mean()<0.1)).any()
        os.remove(csv_name)

remove_old = True
for i in range(istart, len(datasets)):
    if datasets.iloc[i]["TableType"].lower() in ['calls for service', 'incidents', 'crashes']:
        continue

    if random.random()>perc_update:
        continue

    srcName = datasets.iloc[i]["SourceName"]
    state = datasets.iloc[i]["State"]

    agency = None

    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    print(f"{now} Running index {i} of {len(datasets)}: {srcName} {datasets.iloc[i]['TableType']} table")

    src = opd.Source(srcName, state=state, agency=datasets.iloc[i]["Agency"])

    if datasets.iloc[i]['Year']==opd.defs.MULTI and datasets.iloc[i]["DataType"] not in ['CSV','Excel']:
        try:
            years = src.get_years(datasets.iloc[i]["TableType"], datasets=datasets.iloc[i])
            years.sort(reverse=True)
            load_by_year = True
        except (opd.exceptions.OPD_DataUnavailableError, opd.exceptions.OPD_SocrataHTTPError):
            a = 1
        except opd.exceptions.OPD_FutureError:
            continue
    else:
        years = [datasets.iloc[i]['Year']]

    for y in years:
        outfile = src.get_parquet_filename(y, backup_dir, datasets.iloc[i]["TableType"], 
                    agency=agency, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
        
        csv_name = src.get_csv_filename(y, backup_dir, datasets.iloc[i]["TableType"], 
                        agency=agency, url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
        zip_name = csv_name.replace('.csv', '.zip')

        check_old = remove_old and (os.path.exists(csv_name) or os.path.exists(zip_name))

        outfile = outfile.replace('.parquet','.geoparquet') if os.path.exists(outfile.replace('.parquet','.geoparquet')) else outfile
        
        if os.path.exists(outfile):
            df = src.load_parquet(y, table_type=datasets.iloc[i]["TableType"], filename=outfile, 
                                 agency=agency,  url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id']).table
        else:
            table = src.load(datasets.iloc[i]["TableType"], y,
                    agency=agency,  url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id'])
            
            try:
                outfile = table.to_parquet(output_dir=backup_dir, mixed=True)
                if check_old:
                    df = src.load_parquet(y, table_type=datasets.iloc[i]["TableType"], filename=outfile, 
                                    agency=agency,  url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id']).table
            except TypeError as e:
                outfile = outfile.replace('.parquet', '_unicode_error.csv')
                table.to_csv(filename=outfile)
                if check_old:
                    df = src.load_csv(date=y, table_type=datasets.iloc[i]["TableType"], filename=outfile, 
                                    agency=agency,  url=datasets.iloc[i]['URL'], id=datasets.iloc[i]['dataset_id']).table
                
        if check_old:
            if os.path.exists(csv_name):
                df1 = pd.read_csv(csv_name)
                comp(df, df1, csv_name)
                
            if os.path.exists(zip_name):
                df2 = pd.read_csv(zip_name)
                comp(df, df2, zip_name)
                
                if os.path.exists(csv_name):
                    raise NotImplementedError()
        
        if run_single_year_per_dataset:
            break
            
print('Backup complete')     