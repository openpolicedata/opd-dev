import sys
import os
import zipfile
# Assuming this is either running in the root directory or opd-dev inside the root directory
print(os.getcwd())
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","backup")
import openpolicedata as opd
from datetime import datetime
import subprocess

istart = 0
datasets = opd._datasets.datasets_query()

print(output_dir)
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

for i in range(istart, len(datasets)):
    srcName = datasets.iloc[i]["SourceName"]
    state = datasets.iloc[i]["State"]

    table_print = datasets.iloc[i]["TableType"]
    now = datetime.now().strftime("%d.%b %Y %H:%M:%S")
    print(f"{now} Saving CSV for dataset {i} of {len(datasets)}: {srcName} {table_print} table")

    src = opd.Source(srcName, state=state)

    if datasets.iloc[i]["DataType"] ==opd.defs.DataType.CSV.value or \
        datasets.iloc[i]["Year"] != opd.defs.MULTI:
        csv_filename = src.get_csv_filename(datasets.iloc[i]["Year"], output_dir, datasets.iloc[i]["TableType"])
        zipname = csv_filename.replace(".csv",".zip")
        if os.path.exists(zipname):
            continue
        if os.path.exists(csv_filename):
            with zipfile.ZipFile(zipname, mode="w", compression=zipfile.ZIP_LZMA) as archive:
                archive.write(csv_filename)
            if not os.path.exists(zipname):
                raise FileExistsError(zipname)
            os.remove(csv_filename)
            continue
        try:
            table = src.load_from_url(datasets.iloc[i]["Year"], datasets.iloc[i]["TableType"])
        except Exception as e:
            log_to_file(srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], e.args)
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
            if os.path.exists(zipname):
                continue
            if os.path.exists(csv_filename):
                with zipfile.ZipFile(zipname, mode="w", compression=zipfile.ZIP_LZMA) as archive:
                    archive.write(csv_filename)
                if not os.path.exists(zipname):
                    raise FileExistsError(zipname)
                os.remove(csv_filename)
                continue

            print(f"\t{now} Year: {year}")

            try:
                table = src.load_from_url(year, datasets.iloc[i]["TableType"])
            except Exception as e:
                log_to_file(srcName, datasets.iloc[i]["TableType"], datasets.iloc[i]["Year"], year, e.args)
                continue

            csv_filename = table.to_csv(output_dir)
            zipname = csv_filename.replace(".csv",".zip")
            with zipfile.ZipFile(zipname, mode="w", compression=zipfile.ZIP_LZMA) as archive:
                archive.write(csv_filename)
            if not os.path.exists(zipname):
                raise FileExistsError(zipname)
            os.remove(csv_filename)

log_to_file("Completed data backup")