# %%
import os
import sys
from datetime import datetime
import pandas as pd

if os.path.basename(os.getcwd()) == "openpolicedata":
    opd_dir = os.path.join("..","openpolicedata")
    output_dir = os.path.join(".","data","pdap")
else:
    opd_dir = os.path.join("..","..","openpolicedata")
    output_dir = os.path.join("..","data","pdap")
sys.path.append(opd_dir)
import openpolicedata as opd

import matching
import opd2pdap
import opd_consolidation
from pdap_utils import test_url, strip_https_www

assert os.path.exists(output_dir)

kstart = 0

opd_datasets_repo = os.path.join(os.path.dirname(opd_dir), 'opd-data')
opd_source_file = None  # # If None, will use default file from GitHub
opd_source_file = os.path.join(opd_datasets_repo, "opd_source_table.csv")
opd_deleted_file  = os.path.join(opd_datasets_repo, "datasets_deleted_by_publisher.csv")
if opd_source_file!=None:
    opd.datasets.reload(opd_source_file)
df_opd_orig = opd.datasets.query()
df_deleted = pd.read_csv(opd_deleted_file)

orig_cols = df_opd_orig.columns

df_pdap_all = pd.read_csv(os.path.join(output_dir,'PDAP Data Sources_20240705.csv'))

# Only keep Individual record data
df_pdap = df_pdap_all[~df_pdap_all["detail_level"].isin(['Summarized totals','Aggregated records'])]
# We do not currently have PDFs in OPD
df_pdap = df_pdap[~df_pdap['source_url'].str.lower().str.endswith('.pdf', na=False)]
df_pdap = df_pdap[df_pdap['record_type']!='List of Data Sources']

# %% Convert OPD datasets table to 1 row per table type per source

opd_consolidated_file = r'OPD_Source_Table_consolidated.csv'
df_opd_red = opd_consolidation.reduce(df_opd_orig, opd_consolidated_file)


# %%

df_opd = opd2pdap.to_pdap(df_opd_red)

bad_pdap_entries = []
for k in df_opd.index:
    if k<kstart:
        continue

    print("{}: {} {}".format(k, df_opd.loc[k, "SourceName"], df_opd.loc[k, "record_type"]))

    opd_api_urls = [strip_https_www(y.strip()) for y in df_opd.loc[k, 'api_url_all'].split(',')]
    
    src = opd.Source(df_opd.loc[k, "SourceName"], df_opd.loc[k, "State"])

    if 'stanford' in df_opd.loc[k, 'URL']:
        df_opd = matching.match_stanford(df_pdap, df_opd, k)
        continue

    if df_opd.loc[k, "Agency"] == opd.defs.MULTI:
        df_opd = matching.match_multi(df_pdap, df_opd, k)
        continue
    else:
        df_opd = matching.match(df_pdap, df_opd, k)

columns = ['state', 'SourceName', 'Agency','AgencyFull','record_type', 'DataType',
           'new', 'stanford_new', 'OPD Posted Removed Data',
           'source_url', 'update_source_url',
           'scraper_url', 'update_opd_scraper',
           'coverage_start', 'update_coverage_start', 
           'coverage_end', 'update_coverage_end',
           'readme_url','update_readme', 'pdap_readme_url',
           'access_type', 'update_access_type',
           'data_portal_type', 'update_data_portal_type', 
           'agency_aggregation', 'update_agency_aggregation', 
           'record_format', 'update_record_format',
           'supplying_entity', 'update_supplying_entity', 
           'agency_originated', 'update_agency_originated',
            'agency_supplied', 'update_agency_supplied',
            'detail_level', 'update_detail_level', 
            'update_stanford_not_aggregated', 
            'source_url_type', 'airtable_uid', 
           ]

columns = [x for x in columns if x in df_opd]
assert all(x in columns for x in df_opd.columns if x not in orig_cols and x not in ['api_url_all', 'source_url_all'])

df_opd = df_opd[columns]

drop_check_cols = [x for x in columns if x=='new' or x.startswith('update_')]
df_opd = df_opd.dropna(subset=drop_check_cols, how='all')

output_file = os.path.join(output_dir, "Possible_PDAP_Updates_"+datetime.now().strftime("%Y%m%d_%H%M%S")+".csv")
df_opd.to_csv(output_file, index=False)

# TODO: Add code standardizing OPD scraper_url info
# TODO: Incident Reports record type is a catch all with many corrections needed
# TODO: Indicate all PDAP null source_urls