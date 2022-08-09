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

run_all = True
run_all_years = True
run_all_agencies = True
log_result = True
if log_result:
    from openpyxl import Workbook, load_workbook
skip_sources = [("California","STOPS"),("Fayetteville", "ALL"),("Los Angeles County", "ALL")]
istart = 0
datasets = opd._datasets.datasets_query()
if run_all:
    max_num_stanford = float("inf")
else:
    max_num_stanford = 1

xl_dir = os.path.join(output_dir, "standardization")
if not os.path.exists(xl_dir):
    os.mkdir(xl_dir)

def log_to_excel(table):
    xl_filename = os.path.join(xl_dir,
        f"{table.state}_{table.source_name}_{table.agency}_{table.table_type.value}.xlsx"
    )
    if os.path.exists(xl_filename):
        raise NotImplementedError()
    else:
        wb = Workbook()

    sheets = wb.sheetnames
    if sheets == ["Sheet"]:
        # Default val. Rename sheet.
        wb["Sheet"].title = "Columns"

    ws = wb["Columns"]
    ws.cell(row = 1, column = 1).value = str(y)
    ws.cell(row = 2, column = 1).value = "Original"
    ws.cell(row = 2, column = 2).value = "Cleaned"
    row = 2
    for k in range(len(table.table.columns)):
        if not table.table.columns[k].startswith("RAW_"):
            row+=1
            ws.cell(row = row, column = 1).value = table.table.columns[k]
        
    for map in table.clean_hist:
        if type(map.new_column_name) == str:
            loc = [k for k,x in enumerate(ws["A"]) if x.value==map.new_column_name][0]
            ws.cell(row = loc+1, column = 2).value = ws.cell(row = loc+1, column = 1).value
            if type(map.old_column_name) == str:
                ws.cell(row = loc+1, column = 1).value = map.old_column_name
            else:
                ws.cell(row = loc+1, column = 1).value = map.old_column_name[0]
                for name in map.old_column_name[1:]:
                    ws.insert_rows(loc+2)
                    ws.cell(row = loc+2, column = 2).value = ws.cell(row = loc+1, column = 2).value
                    ws.cell(row = loc+2, column = 1).value = name
        else:
            raise TypeError(f"Unknown type for new column name {map.new_column_name}")

    wb.save(xl_filename)

num_stanford = 0
prev_sources = []
prev_tables = []
prev_states = []
output_dir = ".\\data"
move_to_end = []
has_issue = datasets["SourceName"].apply(lambda x : x in move_to_end)
no_issue = datasets["SourceName"].apply(lambda x : x not in move_to_end)
datasets = pd.concat([datasets[no_issue], datasets[has_issue]])
for i in range(istart, len(datasets)):
    if "stanford.edu" in datasets.iloc[i]["URL"]:
        num_stanford += 1
        if num_stanford > max_num_stanford:
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

    if skip or any([x==srcName and (y==datasets.iloc[i]["TableType"] or y=="ALL") for x,y in skip_sources]):
        continue

    prev_sources.append(srcName)
    prev_tables.append(datasets.iloc[i]["TableType"])
    prev_states.append(datasets.iloc[i]["State"])

    table_print = datasets.iloc[i]["TableType"]
    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    print(f"{now} Running dataset {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state)

    if action == "standardize":
        year = date.today().year
        table = None
        csv_filename = "NOT A FILE"
        try:
            years = src.get_years(datasets.iloc[i]["TableType"])
            years.sort(reverse=True)
            load_by_year = True
        except:
            load_by_year = False

        if load_by_year:
            for y in years:
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
                elif run_all_years:
                    backup_dir = os.path.join(output_dir, "backup")
                    csv_filename = src.get_csv_filename(y, "", datasets.iloc[i]["TableType"], agency=agency)
                    zip_filename = csv_filename.replace(".csv",".zip")
                    if not os.path.exists(os.path.join(backup_dir,zip_filename)):
                        # raise FileNotFoundError(f"Backup cannot be found for file {zip_filename}")
                        table = src.load_from_url(y, table_type=datasets.iloc[i]["TableType"], 
                            agency=agency)
                    else:
                        table = src.load_from_csv(y, table_type=datasets.iloc[i]["TableType"], 
                            agency=agency,output_dir=backup_dir, filename=zip_filename)
                    print(f"\t{y} successfully read")
                else:
                    continue
                
                table.clean(keep_raw=True)
                table.merge_date_and_time(ifmissing="ignore")

                if log_result:
                    log_to_excel(table)
                        
                if not run_all_years:
                    break
        else:
            csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], output_dir, datasets.iloc[i]["TableType"], 
                        agency=agency)
            if os.path.exists(csv_filename):
                table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                    agency=agency,output_dir=output_dir)
            elif run_all:
                backup_dir = os.path.join(output_dir, "backup")
                csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], "", datasets.iloc[i]["TableType"], agency=agency)
                zip_filename = csv_filename.replace(".csv",".zip")
                if not os.path.exists(os.path.join(backup_dir,zip_filename)):
                    # raise FileNotFoundError(f"Backup cannot be found for file {zip_filename}")
                    table = src.load_from_url(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                        agency=agency)
                else:
                    table = src.load_from_csv(datasets.iloc[i]["Year"], table_type=datasets.iloc[i]["TableType"], 
                        agency=agency,output_dir=backup_dir, filename=zip_filename)
            else:
                raise FileNotFoundError(f"File {csv_filename} not found")

            table.clean(keep_raw=True)
            table.merge_date_and_time(ifmissing="ignore")

            if log_result:
                log_to_excel(table)
    

print("data main function complete")
