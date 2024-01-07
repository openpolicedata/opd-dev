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

# They are logged to a datestamped file. If the script is run multiple times, cases will 
# only be logged if they have not previously been logged.

############## CONFIGURATION PARAMETERS ###########################

# File locations 
# CSV file containing MPV data downloaded from https://airtable.com/appzVzSeINK1S3EVR/shroOenW19l1m3w0H/tblxearKzw8W7ViN8
csv_filename = os.path.join(file_loc, r"data\MappingPoliceViolence", "Mapping Police Violence_Accessed20240101.csv")
output_dir = os.path.join(file_loc, r"data\MappingPoliceViolence", "Updates", 'tmp') # Where to output cases found

# Names of columns that are not automatically identified
mpv_addr = "street_address"
mpv_state_col = 'state'

# Parameters that affect which cases are logged
min_date = None   # Cases will be ignored before this date. If None, min_date will be set to the oldest date in MPV's data
include_unknown_fatal = False  # Whether to include cases where there was a shooting but it is unknown if it was fatal 
log_demo_diffs = False  # Whether to log cases where a likely match between MPV and OPD cases were found but listed race or gender differs
log_age_diffs = False  # Whether to log cases where a likely match between MPV and OPD cases were found but age differs
# Whether to keep cases that are marked self-inflicted in the data
keep_self_inflicted = False

# Logging and restarting parameters
istart = 0  # 1-based index (same as log statements) to start at in OPD datasets. Can be useful for restarting. Set to 0 to start from beginning.
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
    test_gender_col = opd_table.get_gender_col()
    test_age_col = opd_table.get_age_col()

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
        
        # Remove cases that match between OPD and MPV for cases that have the same date
        # Result is output back to df_opd
        df_opd, mpv_matched, subject_demo_correction, match_with_age_diff = ois_matching.remove_matches_same_date(
            df_mpv_agency, df_opd, mpv_addr, addr_col, test_cols, allowed_replacements)
                
        for j, row_match in df_mpv_agency.iterrows():
            if len(df_opd)==0:
                break
            if mpv_matched[j]:
                continue
            # Look for matches where dates differ
            is_match, _, _ = ois_matching.check_for_match(df_opd, row_match)

            if is_match.sum()>0:
                df_matches = df_opd[is_match]
                if len(df_matches)>1:
                    date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '3d')
                    if addr_col:
                        addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[addr_col], notfound='error')

                    if date_close.sum()==1 and (not addr_col or addr_match[date_close].iloc[0]):
                        df_opd = df_opd.drop(index=df_matches[date_close].index)
                        mpv_matched[j] = True
                    elif not addr_col and \
                        ois_matching.in_date_range(df_matches[date_col], row_match[date_col], min_delta='9d').all():
                        continue
                    elif addr_col and (not addr_match.any() or \
                        ois_matching.in_date_range(df_matches[addr_match][date_col], row_match[date_col], min_delta='300d').all()):
                        continue
                    else:
                        raise NotImplementedError()
                elif not addr_col:
                    if ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '2d').iloc[0]:
                        df_opd = df_opd.drop(index=df_matches.index)
                        mpv_matched[j] = True
                    elif ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '11d').iloc[0]:
                        if ois_matching.zipcode_isequal(row_match, df_matches, iloc2=0):
                            # row_match[zip_col]==df_matches[zip_col].iloc[0]:
                            df_opd = df_opd.drop(index=df_matches.index)
                            mpv_matched[j] = True
                        elif ois_matching.zipcode_isequal(row_match, df_matches, iloc2=0, count='none'):
                            # zip_col and zip_col and df_matches.iloc[0][zip_col]!=row_match[zip_col]:
                            continue
                        else:
                            raise NotImplementedError()
                    elif ois_matching.in_date_range(df_matches[date_col],row_match[date_col], min_delta='30d').iloc[0]:
                        continue
                    elif ois_matching.zipcode_isequal(row_match, df_matches, iloc2=0, count='none'):
                    #zip_col and zip_col and df_matches.iloc[0][zip_col]!=row_match[zip_col]:
                        continue
                    else:
                        raise NotImplementedError()
                else:
                    date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '3d').iloc[0]
                    addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[addr_col], notfound='error').iloc[0]
                    
                    if date_close and addr_match:
                        df_opd = df_opd.drop(index=df_opd[is_match].index)
                        mpv_matched[j] = True
                    elif addr_match and ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '31d', '30d').iloc[0]:
                        # Likely error in the month that was recorded
                        df_opd = df_opd.drop(index=df_opd[is_match].index)
                        mpv_matched[j] = True
                    elif addr_match:
                        raise NotImplementedError()
                    elif ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '110d').iloc[0]:
                        raise NotImplementedError()

        j = 0
        while j<len(df_opd):
            if len(df_mpv_agency)==0:
                break
            
            mpv_unmatched = df_mpv_agency[~mpv_matched]
            if not addr_col:
                date_close = ois_matching.in_date_range(df_opd.iloc[j][date_col], mpv_unmatched[date_col], '5d')
                if not date_close.any():
                    j+=1
                    continue
                if isinstance(df_opd.iloc[j][date_col], pd.Period):
                    j+=1
                    continue
                
                date_diff = abs(mpv_unmatched[date_close][date_col] - df_opd.iloc[j][date_col])
                if zip_col in mpv_unmatched and zip_col in df_opd:
                    if not (zip_matches:=mpv_unmatched[date_close][zip_col]==df_opd.iloc[j][zip_col]).any():
                        j+=1  # No zip codes match
                        continue
                    if (m:=(date_diff[zip_matches]<='4d')).any():  # Zip codes do match
                        is_match, _, _ = ois_matching.check_for_match(
                            mpv_unmatched[date_close][zip_matches][m], df_opd.iloc[j], 
                            max_age_diff=5, allowed_replacements=allowed_replacements)
                        if is_match.sum()==1:
                            match_with_age_diff[is_match[is_match].index[0]] = df_opd.iloc[j]
                            df_opd = df_opd.drop(index=df_opd.index[j])
                            mpv_matched[is_match[is_match].index[0]] = True
                            continue
                        elif test_gender_col in df_opd and df_opd.iloc[j][test_gender_col]=='FEMALE' and \
                            (mpv_unmatched[date_close][mpv_gender_col]=="MALE").all():
                            j+=1
                            continue

                raise NotImplementedError()

            matches = ois_matching.street_match(df_opd.iloc[j][addr_col], addr_col, mpv_unmatched[mpv_addr], notfound='error')

            if matches.any():
                date_close = ois_matching.in_date_range(df_opd.iloc[j][date_col], mpv_unmatched[matches][date_col], '3d')
                if date_close.any():
                    if date_close.sum()>1:
                        raise NotImplementedError()
                    date_close = [k for k,x in date_close.items() if x][0]
                    # Consider this a match with errors in the demographics
                    if date_close in subject_demo_correction:
                        raise NotImplementedError("Attempting demo correction twice")
                    subject_demo_correction[date_close] = df_opd.iloc[j]
                    df_opd = df_opd.drop(index=df_opd.index[j])
                    mpv_matched[date_close] = True
                elif (ois_matching.in_date_range(df_opd.iloc[j][date_col], mpv_unmatched[matches][date_col], '150d')).any() and \
                    (len(mpv_unmatched[matches])>1 or address_parser.tag(mpv_unmatched[mpv_addr][matches].iloc[0], mpv_addr)[1]!='Coordinates'):
                    match_sum = \
                        (mpv_unmatched[matches>0][mpv_race_col] == df_opd.iloc[j][opd_race_col]).apply(lambda x: 1 if x else 0) + \
                        (mpv_unmatched[matches>0][mpv_age_col] == df_opd.iloc[j][test_age_col]).apply(lambda x: 1 if x else 0) + \
                        (mpv_unmatched[matches>0][mpv_gender_col] == df_opd.iloc[j][test_gender_col]).apply(lambda x: 1 if x else 0)
                    if mpv_unmatched[matches>0]['officer_names'].notnull().any():
                        raise NotImplementedError("Check this")
                    elif (abs(df_opd.iloc[j][date_col]-mpv_unmatched[matches>0][date_col])<'30d').any():
                        raise NotImplementedError("Check this")
                    elif (match_sum==3).any():
                        raise NotImplementedError("Check this")

            j+=1

        if addr_col:
            # Check for cases where shooting might be listed under another agency or MPV agency might be null
            mpv_state = agencyutils.filter_state(df_mpv, mpv_state_col, row_dataset['State'])
            # Remove cases that have already been checked
            mpv_state = mpv_state.drop(index=df_mpv_agency.index)
            j = 0
            while j<len(df_opd):
                addr_match = ois_matching.street_match(df_opd.iloc[j][addr_col], addr_col, mpv_state[mpv_addr], 
                                                       notfound='error', match_col_null=False)
                if addr_match.any() and \
                    ois_matching.in_date_range(df_opd.iloc[j][date_col], mpv_state[addr_match][date_col], '30d').any():
                    if (m:=ois_matching.in_date_range(df_opd.iloc[j][date_col], mpv_state[addr_match][date_col], '1d')).any():
                        is_match, _, _ = ois_matching.check_for_match(mpv_state.loc[addr_match[addr_match][m].index], df_opd.iloc[j])
                        if is_match.any():
                            df_opd = df_opd.drop(index=df_opd.index[j])
                            continue
                        else:
                            raise NotImplementedError()
                    else:
                        raise NotImplementedError()
                    
                j+=1

        df_save = []
        if len(df_opd)>0:
            df_opd['type'] = 'Unmatched'
            df_save.append(df_opd)

        if log_demo_diffs and len(subject_demo_correction)>0:
            df = pd.DataFrame(subject_demo_correction).transpose()
            df['type'] = 'Demo Correction?'
            # Create hash of MPV row
            df['MPV Hash'] = df_mpv_agency.loc[df.index].apply(
                lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
                )

            df["MPV Row"] = df.index
            df["MPV ID"] = df_mpv_agency.loc[df.index]['mpv_id']
            df["MPV DATE"] = df_mpv_agency.loc[df.index][date_col]
            df["MPV RACE"] = df_mpv_agency.loc[df.index][mpv_race_col]
            df["MPV GENDER"] = df_mpv_agency.loc[df.index][mpv_gender_col]
            df["MPV AGE"] = df_mpv_agency.loc[df.index][mpv_age_col]
            df["MPV AGENCY"] = df_mpv_agency.loc[df.index][agency_col]
            df["MPV ADDRESS"] = df_mpv_agency.loc[df.index][mpv_addr]
            df_save.append(df)
        
        if log_age_diffs and len(match_with_age_diff)>0:
            df = pd.DataFrame(match_with_age_diff).transpose()
            df['type'] = 'Age Difference'
            # Create hash of MPV row
            df['MPV Hash'] = df_mpv_agency.loc[df.index].apply(
                lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
                )

            df["MPV Row"] = df.index
            df["MPV ID"] = df_mpv_agency.loc[df.index]['mpv_id']
            df["MPV DATE"] = df_mpv_agency.loc[df.index][date_col]
            df["MPV RACE"] = df_mpv_agency.loc[df.index][mpv_race_col]
            df["MPV GENDER"] = df_mpv_agency.loc[df.index][mpv_gender_col]
            df["MPV AGE"] = df_mpv_agency.loc[df.index][mpv_age_col]
            df["MPV AGENCY"] = df_mpv_agency.loc[df.index][agency_col]
            df["MPV ADDRESS"] = df_mpv_agency.loc[df.index][mpv_addr]
            df_save.append(df)

        if len(df_save)>0:
            df_save = pd.concat(df_save, ignore_index=True)
            df_save['MPV Download Date'] = mpv_download_date
            df_save['Agency'] = agency
            df_save['known_fatal'] = known_fatal

            keys = ["MPV ID", 'type', 'known_fatal', 'Agency', date_col]
            if opd_race_col  in df_save:
                keys.append(opd_race_col )
            if test_gender_col in df_save:
                keys.append(test_gender_col)
            if test_age_col  in df_save:
                keys.append(test_age_col )
            if addr_col:
                keys.append(addr_col)

            new_cols = ['type', 'known_fatal', 'Agency']
            mpv_cols = [x for x in df_save.columns if x.lower().startswith("mpv")]
            new_cols.extend(mpv_cols)
            new_cols.extend([k for k in keys if k not in new_cols and k in df_save])
            new_cols.extend([x for x in df_save.columns if x not in new_cols])
            df_save = df_save[new_cols]

            # Save data specific to this source
            source_basename = f"{row_dataset['SourceName']}_{row_dataset['State']}_{row_dataset['TableType']}_{row_dataset['Year']}"
            opd_logger.log(df_save, output_dir, source_basename, keys=keys, add_date=True, only_diffs=True)

            cols = ['type', 'known_fatal']
            for c in df_save.columns:
                if c.startswith("MPV"):
                    cols.append(c)

            df_global = df_save[cols].copy()
            df_global["OPD Date"] = df_save[date_col]
            df_global["OPD Agency"] = df_save['Agency']

            if opd_race_col  in df_save:
                df_global["OPD Race"] = df_save[opd_race_col ]
            if test_gender_col in df_save:
                df_global["OPD Gender"] = df_save[test_gender_col]
            if test_age_col  in df_save:
                df_global["OPD Age"] = df_save[test_age_col]
            if addr_col:
                df_global["OPD Address"] = df_save[addr_col]

            # CSV file containing all recommended updates with a limited set of columns
            global_basename = 'Potential_MPV_Updates_Global'
            keys = ["MPV ID", 'type', 'known_fatal', 'OPD Date','OPD Agency','OPD Race', 'OPD Gender','OPD Age','OPD Address']
            opd_logger.log(df_global, output_dir, global_basename, keys=keys, add_date=True, only_diffs=True)
