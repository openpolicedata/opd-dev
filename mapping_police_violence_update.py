import pandas as pd
import os, sys
from hashlib import sha1
from datetime import datetime
file_loc = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_loc)  # Add current file directory to path
from opddev.utils import address_parser
from opddev.utils import agencyutils
from opddev.utils import ois_matching
from opddev.utils import opd_logger
import openpolicedata as opd
import logging

# TODO: Remove NotImplementedError
# TODO: Remove error inputs

# They are logged to a datestamped file. If the script is run multiple times, cases will 
# only be logged if they have not previously been logged.

############## CONFIGURATION PARAMETERS ###########################

# File locations 
# CSV file containing MPV data downloaded from https://airtable.com/appzVzSeINK1S3EVR/shroOenW19l1m3w0H/tblxearKzw8W7ViN8
csv_filename = os.path.join(file_loc, r"data\MappingPoliceViolence", "Mapping Police Violence_Accessed20240101.csv")
output_dir = os.path.join(file_loc, r"data\MappingPoliceViolence", "Updates", 'tmp') # Where to output cases found

# Names of columns that are not automatically identified
mpv_addr_col = "street_address"
mpv_state_col = 'state'

# Parameters that affect which cases are logged
min_date = None   # Cases will be ignored before this date. If None, min_date will be set to the oldest date in MPV's data
include_unknown_fatal = False  # Whether to include cases where there was a shooting but it is unknown if it was fatal 
log_demo_diffs = False  # Whether to log cases where a likely match between MPV and OPD cases were found but listed race or gender differs
log_age_diffs = False  # Whether to log cases where a likely match between MPV and OPD cases were found but age differs
# Whether to keep cases that are marked self-inflicted in the data
keep_self_inflicted = False

# Logging and restarting parameters
istart = 1  # 1-based index (same as log statements) to start at in OPD datasets. Can be useful for restarting. Set to 0 to start from beginning.
logging_level = logging.INFO  # Logging level. Change to DEBUG for some additional messaging.

# There are sometimes demographic differences between MPV and other datasets for the same case
# If a perfect demographics match is not found, an attempt can be made to allow differences in race and gender values
# when trying to find a case with matching demographics. The below cases have been identified  as differing in some cases.
# The pairs below will be considered equivalent if allowed_replacements is used
allowed_replacements = {'race':[["HISPANIC/LATINO","INDIGENOUS"],["HISPANIC/LATINO","WHITE"],["HISPANIC/LATINO","BLACK"],
                                ['ASIAN','ASIAN/PACIFIC ISLANDER']],
                        'gender':[['TRANSGENDER','MALE'],['TRANSGENDER','FEMALE']]}
    
####################################################################

try:
    # Attempt to pull date downloaded out of input file
    mpv_download_date = datetime.strptime(csv_filename[-4-8:-4], '%Y%m%d')
except:
    mpv_download_date = ''

logger = ois_matching.get_logger(logging_level)

# Convert to OPD table so that standardization can be applied and some column names and terms for race and gender can be standardized
mpv_raw = pd.read_csv(csv_filename)
mpv_table = opd.data.Table({"SourceName":"Mapping Police Violence", 
                      "State":opd.defs.MULTI, 
                      "TableType":opd.defs.TableType.SHOOTINGS}, 
                     mpv_raw,
                     opd.defs.MULTI)
mpv_table.standardize(known_cols={opd.defs.columns.AGENCY:"agency_responsible"})
df_mpv = mpv_table.table  # Retrieve pandas DataFrame from Table class

# Standard column names for all datasets that have these columns
date_col = opd.defs.columns.DATE
agency_col = opd.defs.columns.AGENCY
fatal_col = opd.defs.columns.FATAL_SUBJECT
role_col = opd.defs.columns.SUBJECT_OR_OFFICER
injury_cols = [opd.defs.columns.INJURY_SUBJECT, opd.defs.columns.INJURY_OFFICER_SUBJECT]
zip_col = opd.defs.columns.ZIP_CODE

# Standard column names for MPV
mpv_race_col = ois_matching.get_race_col(df_mpv)
mpv_gender_col = ois_matching.get_gender_col(df_mpv)
mpv_age_col = ois_matching.get_age_col(df_mpv)

min_date = pd.to_datetime(min_date) if min_date else df_mpv[date_col].min()

# Get a list of officer-involved shootings and use of force tables in OPD
tables_to_use = [opd.defs.TableType.SHOOTINGS, opd.defs.TableType.SHOOTINGS_INCIDENTS,
                 opd.defs.TableType.USE_OF_FORCE, opd.defs.TableType.USE_OF_FORCE_INCIDENTS]
opd_datasets = []
for t in tables_to_use:
    opd_datasets.append(opd.datasets.query(table_type=t))
opd_datasets = pd.concat(opd_datasets, ignore_index=True)
logger.info(f"{len(opd_datasets)} officer-involved shootings or use of force datasets found in OPD")

for k, row_dataset in opd_datasets.iloc[max(1,istart)-1:].iterrows():  # Loop over OPD OIS datasets
    logger.info(f'Running {k+1} of {len(opd_datasets)}: {row_dataset["SourceName"]} {row_dataset["TableType"]} {row_dataset["Year"] if row_dataset["Year"]!="MULTIPLE" else ""}')

    # Load this OPD dataset
    src = opd.Source(row_dataset["SourceName"], state=row_dataset["State"])    # Create source for agency
    opd_table = src.load_from_url(row_dataset['Year'], row_dataset['TableType'])  # Load data
    opd_table.standardize(agg_race_cat=True)  # Standardize data
    opd_table.expand(mismatch='splitsingle')  # Expand cases where the info for multiple people are contained in the same row
    # Some tables contain incident information in 1 table and subject and/or officer information in other tables
    related_table, related_years = src.find_related_tables(opd_table.table_type, opd_table.year, sub_type='SUBJECTS')
    if related_table:
        t2 = src.load_from_url(related_years[0], related_table[0])
        t2.standardize(agg_race_cat=True)
        try:
            # Merge incident and subjects tables on their unique ID columns to create 1 row per subject
            opd_table = opd_table.merge(t2, std_id=True)
        except ValueError as e:
            if len(e.args)>0 and e.args[0]=='No incident ID column found' and \
                row_dataset["SourceName"]=='Charlotte-Mecklenburg':
                # Dataset has no incident ID column. Latitude/longitude seems to work instead
                opd_table = opd_table.merge(t2, on=['Latitude','Longitude'])
            else:
                raise
        except:
            raise
    df_opd_all = opd_table.table

    # Get standardized demographics columns for OPD data
    opd_race_col = opd_table.get_race_col()
    opd_gender_col = opd_table.get_gender_col()
    opd_age_col = opd_table.get_age_col()

    df_opd_all, known_fatal, test_cols = ois_matching.clean_data(opd_table, df_opd_all, row_dataset['TableType'], injury_cols, fatal_col, min_date, 
                                                  include_unknown_fatal, keep_self_inflicted)

    if len(df_opd_all)==0:
        continue  # No data move to the next dataset

    # Find address or street column if it exists
    addr_col = address_parser.find_address_col(df_opd_all)
    addr_col = addr_col[0] if len(addr_col)>0 else None

    # If dataset has multiple agencies, loop over them individually
    agency_names = df_opd_all[opd.defs.columns.AGENCY].unique() if row_dataset['Agency']==opd.defs.MULTI else [row_dataset['AgencyFull']]
    for agency in agency_names:
        if row_dataset['Agency']==opd.defs.MULTI:
            df_opd = df_opd_all[df_opd_all[opd.defs.columns.AGENCY]==agency].copy()
        else:
            df_opd = df_opd_all.copy()

        # Get the location (agency_partial) and type (police department, sheriff's office, etc.) from the agency
        agency_partial, agency_type = agencyutils.split(agency, row_dataset['State'], unknown_type='error')
        df_mpv_agency = agencyutils.filter_agency(agency, agency_partial, agency_type, row_dataset['State'], 
                                             df_mpv, agency_col, mpv_state_col, logger=logger)        

        # First require a perfect demographics match then loosen demographics matching requirements
        # There are differences between the OPD datasets and MPV due to mistakes and other issues
        # See ois_matching.check_for_match for definitions of the different methods for loosening 
        # demographics matching requirements
        args = [{}, {'max_age_diff':1,'check_race_only':True}, 
                {'allowed_replacements':allowed_replacements},
                {'inexact_age':True}, {'max_age_diff':5}, {'allow_race_diff':True},{'max_age_diff':20, 'zip_match':True},
                {'max_age_diff':10, 'zip_match':True, 'allowed_replacements':allowed_replacements}]
        subject_demo_correction = {}
        match_with_age_diff = {}
        mpv_matched = pd.Series(False, df_mpv_agency.index)
        for a in args:
            # Remove cases that match between OPD and MPV for cases that have the same date
            df_opd, mpv_matched, subject_demo_correction, match_with_age_diff = ois_matching.remove_matches_date_match_first(
                df_mpv_agency, df_opd, mpv_addr_col, addr_col, 
                mpv_matched, subject_demo_correction, match_with_age_diff, a, 
                test_cols)
        
        df_opd, mpv_matched = ois_matching.remove_matches_demographics_match_first(df_mpv_agency, df_opd, mpv_addr_col, addr_col, mpv_matched)

        if addr_col:
            df_opd, mpv_matched, subject_demo_correction = ois_matching.remove_matches_street_match_first(df_mpv_agency, df_opd, mpv_addr_col, addr_col,
                                      mpv_matched, subject_demo_correction)
            df_opd = ois_matching.remove_matches_agencymismatch(df_mpv, df_mpv_agency, df_opd, mpv_state_col, mpv_addr_col, addr_col, row_dataset['State'])
        else:
            df_opd, mpv_matched, match_with_age_diff = ois_matching.remove_matches_close_date_match_zipcode(df_mpv_agency, df_opd, mpv_matched, allowed_replacements, match_with_age_diff)
                
        df_save, keys = opd_logger.generate_agency_output_data(df_mpv_agency, df_opd, mpv_addr_col, addr_col, mpv_download_date,
                                log_demo_diffs, subject_demo_correction, log_age_diffs, match_with_age_diff, agency, known_fatal)
        
        if len(df_save)>0:
            # Save data specific to this source
            source_basename = f"{row_dataset['SourceName']}_{row_dataset['State']}_{row_dataset['TableType']}_{row_dataset['Year']}"
            opd_logger.log(df_save, output_dir, source_basename, keys=keys, add_date=True, only_diffs=True)

            df_global = opd_logger.generate_general_output_data(df_save, addr_col)

            # CSV file containing all recommended updates with a limited set of columns
            global_basename = 'Potential_MPV_Updates_Global'
            keys = ["MPV ID", 'type', 'known_fatal', 'OPD Date','OPD Agency','OPD Race', 'OPD Gender','OPD Age','OPD Address']
            opd_logger.log(df_global, output_dir, global_basename, keys=keys, add_date=True, only_diffs=True)
