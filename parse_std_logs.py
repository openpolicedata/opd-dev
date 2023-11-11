from glob import glob
from datetime import datetime
import re
import os

min_date = datetime(2023, 11, 7)

files = glob(r"C:\Users\matth\repos\openpolicedata\data\backup\standardization\std_log*.txt")

log_files = []
for f in files:
    if (m:=re.search(r"std_log_(\d{8}_\d{6})\.txt", f)) and (d:=datetime.strptime(m.group(1), "%Y%m%d_%H%M%S"))>min_date:
        log_files.append(f)

out_file = os.path.join(os.path.dirname(files[0]), f'std_log_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
with open(out_file, 'w') as fout:
    for f in log_files:
        with open(f, 'r') as f:
            for line in f:
                if m:=re.search(r"Running index \d+ of \d+: (.+)\s(.+)\stable", line):
                    src = m.group(1)
                    table_type = m.group(2)
                    new = True
                elif m:=re.search(r":: Year: (\d{4})", line):
                    year = int(m.group(1))
                    new = True
                elif re.search(r":: Year: NONE", line):
                    year = "NONE"
                    new = True
                else:
                    if new:
                        fout.write(f"Agency: {src}\n")
                        fout.write(f"Table: {table_type}\n")
                        fout.write(f"Year: {year}\n")
                    fout.write(line)
                    new = False
