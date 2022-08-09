'''
Script for providing summary data for avaiable data in OPD such as number of states and PDs data that are provided
'''

import os
import sys
if os.path.basename(os.getcwd()) == "openpolicedata":
    sys.path.append(os.path.join("..","openpolicedata"))
    output_dir = os.path.join(".","data","backup")
else:
    sys.path.append(os.path.join("..","..","openpolicedata"))
    output_dir = os.path.join("..","data","backup")
import openpolicedata as opd

src_file = "..\opd-data\opd_source_table.csv"

if src_file != None:
    opd._datasets.datasets = opd._datasets._build(src_file)

datasets = opd.datasets_query()

d = datasets.drop_duplicates(subset=["State","SourceName","Agency"])

# Get the # of entire states
state_datasets = d[(d["State"]==d["SourceName"]) & (d["Agency"]==opd.defs.MULTI)]
state_types = state_datasets["TableType"].unique()
print(f"OPD provides data for {len(state_datasets)} states ({state_types})")
print(f"OPD provides data for {len(d)-len(state_datasets)} police departments, sheriff's departments, and state police")

unique_ds = datasets.drop_duplicates(subset=["State","SourceName","Agency","TableType"])
print(f"OPD provides data from {len(unique_ds)} unique datasets")

