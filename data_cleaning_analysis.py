import os
import sys
from datetime import date
import pandas as pd
from datetime import datetime
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","backup")
import openpolicedata as opd

istart = 0
datasets = opd._datasets.datasets_query()
max_num_stanford = 1
num_stanford = 0
prev_sources = []
prev_tables = []
output_dir = ".\\data"
action = "standardize"
issue_datasets = [ "Chapel Hill", "Fayetteville", "San Diego","New Orleans","Orlando","Jacksonville","Los Angeles County","Norfolk"]
has_issue = datasets["SourceName"].apply(lambda x : x in issue_datasets)
no_issue = datasets["SourceName"].apply(lambda x : x not in issue_datasets)
datasets = pd.concat([datasets[no_issue], datasets[has_issue]])
for i in range(istart, len(datasets)):
    if "stanford.edu" in datasets.iloc[i]["URL"]:
        num_stanford += 1
        if num_stanford > max_num_stanford:
            continue

    srcName = datasets.iloc[i]["SourceName"]
    state = datasets.iloc[i]["State"]

    if datasets.iloc[i]["Agency"] == opd.defs.MULTI and srcName == "Virginia":
        # Reduce size of data load by filtering by agency
        agency = "Fairfax County Police Department"
    else:
        agency = None

    skip = False
    for k in range(len(prev_sources)):
        if srcName == prev_sources[k] and datasets.iloc[i]["TableType"] ==prev_tables[k]:
            skip = True

    if skip:
        continue

    prev_sources.append(srcName)
    prev_tables.append(datasets.iloc[i]["TableType"])

    table_print = datasets.iloc[i]["TableType"]
    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    print(f"{now} Saving CSV for dataset {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state)

    if action == "standardize":
        year = date.today().year
        table = None
        csv_filename = "NOT A FILE"
        for y in range(year, year-20, -1):
            try:
                csv_filename = src.get_csv_filename(y, output_dir, datasets.iloc[i]["TableType"], 
                    agency=agency)
            except ValueError as e:
                if "There are no sources matching tableType" in e.args[0]:
                    continue
                else:
                    raise
            except:
                raise
            
            if os.path.exists(csv_filename):
                table = src.load_from_csv(y, table_type=datasets.iloc[i]["TableType"], 
                    agency=agency,output_dir=output_dir)
                break

        if not os.path.exists(csv_filename):
            try:
                table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                        agency=agency,output_dir=output_dir)
            except Exception as e:
                if srcName == "Fayetteville":
                    continue
                else:
                    raise e

        table.standardize()
        table.merge_date_and_time(ifmissing="ignore")
    else:
        if datasets.iloc[i]["DataType"] ==opd.defs.DataType.CSV.value:
            csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], output_dir, datasets.iloc[i]["TableType"])
            if os.path.exists(csv_filename):
                continue
            table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
        else:
            try:
                years = src.get_years(datasets.iloc[i]["TableType"])
            except:
                continue
            
            if len(years)>1:
                # It is preferred to to not use first or last year that start and stop of year are correct
                year = years[-2]
            else:
                year = years[0]

            csv_filename = src.get_csv_filename(year, output_dir, datasets.iloc[i]["TableType"], 
                                    agency=agency)

            if os.path.exists(csv_filename):
                continue

            table = src.load_from_url(year, datasets.iloc[i]["TableType"], 
                                    agency=agency)

        table.to_csv(".\\data")

print("data main function complete")
