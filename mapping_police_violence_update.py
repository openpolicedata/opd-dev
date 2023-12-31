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

istart = 0
logging_level = logging.DEBUG
include_unknown_fatal = True
include_close_date_matching_zip = True
allowed_replacements = {'race':[["HISPANIC/LATINO","INDIGENOUS"],["HISPANIC/LATINO","WHITE"],["HISPANIC/LATINO","BLACK"],
                                ['ASIAN','ASIAN/PACIFIC ISLANDER']],
                        'gender':[['TRANSGENDER','MALE'],['TRANSGENDER','FEMALE']]}

opd.datasets.reload(r"..\opd-data\opd_source_table.csv")

mpv_folder = os.path.join(file_loc, r"data\MappingPoliceViolence")
output_dir = os.path.join(mpv_folder, "Updates")
mpv_csv_filename = "Mapping Police Violence_Accessed20231111.csv"
mpv_download_date = datetime.strptime(mpv_csv_filename[-4-8:-4], '%Y%m%d')
mpv_addr = "street_address"
mpv_raw = pd.read_csv(os.path.join(mpv_folder, mpv_csv_filename))

logger = logging.getLogger("ois")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# Convert to OPD table so that standardization can be applied to so that terms for race and gender match OPD-loaded tables
mpv = opd.data.Table({"SourceName":"Mapping Police Violence", 
                      "State":opd.defs.MULTI, 
                      "TableType":opd.defs.TableType.SHOOTINGS}, 
                     mpv_raw,
                     opd.defs.MULTI)
mpv.standardize(known_cols={opd.defs.columns.AGENCY:"agency_responsible"})
mpv = mpv.table

date_col = opd.defs.columns.DATE
mpv_race_col = ois_matching.get_race_col(mpv)
mpv_gender_col = ois_matching.get_gender_col(mpv)
mpv_age_col = ois_matching.get_age_col(mpv)
agency_col = opd.defs.columns.AGENCY
fatal_col = opd.defs.columns.FATAL_SUBJECT
role_col = opd.defs.columns.SUBJECT_OR_OFFICER
injury_cols = [opd.defs.columns.INJURY_SUBJECT, opd.defs.columns.INJURY_OFFICER_SUBJECT]
zip_col = opd.defs.columns.ZIP_CODE

# Assuming MPV not looking for OIS before this date
min_date = mpv[date_col].min()

# Get a list of officer-involved shootings and use of force tables
tables_to_use = [opd.defs.TableType.SHOOTINGS, opd.defs.TableType.SHOOTINGS_INCIDENTS,
                 opd.defs.TableType.USE_OF_FORCE, opd.defs.TableType.USE_OF_FORCE_INCIDENTS]
df_ois = []
for t in tables_to_use:
    df_ois.append(opd.datasets.query(table_type=t))
df_ois = pd.concat(df_ois, ignore_index=True)

logger.debug(f"{len(df_ois)} datasets found")
for k, row in df_ois.iloc[istart:].iterrows():  # Loop over OPD OIS datasets
    logger.debug(f'Running {k} of {len(df_ois)-1}: {row["SourceName"]} {row["TableType"]} {row["Year"] if row["Year"]!="MULTIPLE" else ""}')
    src = opd.Source(row["SourceName"], state=row["State"])    # Create source for agency

    is_use_of_force_table = "USE OF FORCE" in row['TableType']
    
    t = src.load_from_url(row['Year'], row['TableType'])  # Load data
    t.standardize(agg_race_cat=True)
    t.expand(mismatch='splitsingle')
    related_table, related_years = src.find_related_tables(t.table_type, t.year, sub_type='SUBJECTS')
    if related_table:
        t2 = src.load_from_url(related_years[0], related_table[0])
        t2.standardize(agg_race_cat=True)
        try:
            t = t.merge(t2, std_id=True)
        except ValueError as e:
            if len(e.args)>0 and e.args[0]=='No incident ID column found' and \
                row["SourceName"]=='Charlotte-Mecklenburg':
                # Dataset has no incident ID column. Latitude/longitude seem to work instead
                t = t.merge(t2, on=['Latitude','Longitude'])
            else:
                raise
        except:
            raise
    df_table = t.table
    test_race_col = t.get_race_col()
    test_gender_col = t.get_gender_col()
    test_age_col = t.get_age_col()

    df_table = ois_matching.remove_officer_rows(df_table)

    is_multi_subject = False
    known_fatal = True
    if fatal_col in df_table:
        fatal_values = ['YES',"UNSPECIFIED",'SELF-INFLICTED FATAL'] if include_unknown_fatal and not is_use_of_force_table else ['YES','SELF-INFLICTED FATAL']
        df_table = df_table[df_table[fatal_col].isin(fatal_values)]
    elif len(c:=[x for x in injury_cols if x in df_table])>0:
        df_table = df_table[df_table[c[0]]=='FATAL']
    else:
        if not include_unknown_fatal or is_use_of_force_table:
            continue
        known_fatal = False

    if len(df_table)==0:
        continue

    if date_col not in df_table:
        raise NotImplementedError("Need to handle data without date")
    
    df_table = ois_matching.filter_by_date(df_table.copy(), date_col, min_date)

    test_cols, ignore_cols = ois_matching.columns_for_duplicated_check(t, df_table)
    df_table = ois_matching.drop_duplicates(df_table, subset=test_cols)

    addr_col = address_parser.find_address_col(df_table)
    addr_col = addr_col[0] if len(addr_col)>0 else None

    if row['Agency']==opd.defs.MULTI:
        agency_names = df_table[opd.defs.columns.AGENCY].unique()
    else:
        agency_names = [row['AgencyFull']]

    for agency in agency_names:
        if row['Agency']==opd.defs.MULTI:
            df_test = df_table[df_table[opd.defs.columns.AGENCY]==agency].copy()
        else:
            df_test = df_table.copy()

        agency_partial, agency_type = agencyutils.split(agency, row['State'], unknown_type='error')
        mpv_agency = agencyutils.find_agency(agency, agency_partial, agency_type, row['State'], 
                                             mpv, agency_col, 'state', logger=logger)

        subject_demo_correction = {}
        match_with_age_diff = {}
        mpv_matched = pd.Series(False, mpv_agency.index)
        for j, row_match in mpv_agency.iterrows():
            df_matches = ois_matching.find_date_matches(df_test, date_col, row_match[date_col])

            if len(df_matches)==0:
                continue

            # Run ois_matching.check_for_match multiple times. Loosen requirements for a match each time
            args = [{}, {'max_age_diff':1,'check_race_only':True}, 
                    {'allowed_replacements':allowed_replacements},
                    {'inexact_age':True}, {'max_age_diff':5}, {'allow_race_diff':True},{'max_age_diff':20, 'zip_match':True},
                    {'max_age_diff':10, 'zip_match':True, 'allowed_replacements':allowed_replacements}]
            age_diff = pd.Series(False, index=df_matches.index)
            for a in args:
                is_match, is_unknown, is_diff_race = ois_matching.check_for_match(df_matches, row_match,**a)
                if is_match.any():
                    age_diff[is_match] = 'max_age_diff' in a.keys()
                    break

            if is_match.sum()==0:
                if len(df_matches)==1 or \
                    ois_matching.zipcode_isequal(df_matches, row_match, count='none'):
                    # (zip_col and zip_col and (df_matches[zip_col]!=row_match[zip_col]).all())
                    logger.warning(f"Match found for {row_match[date_col]} but demographics do not match")
                    continue
                
                raise NotImplementedError()

            if is_match.sum()>1:
                throw = True
                summary_col = [x for x in df_matches.columns if 'summary' in x.lower()]
                if addr_col:
                    test_cols_reduced = test_cols.copy()
                    test_cols_reduced.remove(addr_col)
                    [test_cols_reduced.remove(x) for x in summary_col]
                    if len(ois_matching.drop_duplicates(df_matches[is_match], subset=test_cols_reduced, ignore_null=True, ignore_date_errors=True))==1:
                        # These are the same except for the address. Check if the addresses are similar
                        addr_match = ois_matching.street_match(df_matches[is_match][addr_col].iloc[0], addr_col, 
                                                            df_matches[is_match][addr_col].iloc[1:], notfound='error')
                        throw = not addr_match.all()
                    if throw:
                        #Check if address only matches one case
                        addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[is_match][addr_col], notfound='error')
                        throw = addr_match.sum()!=1
                        if not throw:
                            is_match = addr_match
                elif ois_matching.zipcode_isequal(row_match, df_matches[is_match], count=1):
                # elif zip_col and zip_col and (m:=row_match[zip_col]==df_matches[is_match][zip_col]).sum()==1:
                    is_match.loc[is_match] = row_match[zip_col]==df_matches[is_match][zip_col]
                    throw = False
                if throw:
                    if len(summary_col)>0:
                        throw = not (df_matches[summary_col]==df_matches[summary_col].iloc[0]).all().all()
                if throw:
                    raise NotImplementedError("Multiple matches found")
                    
            for idx in df_matches.index:
                # OIS found in data. Remove from df_test.
                if is_match[idx]: 
                    if is_unknown[idx] or is_diff_race[idx]:
                        subject_demo_correction[j] = df_matches.loc[idx]
                    if age_diff[idx]:
                        if j in match_with_age_diff:
                            raise NotImplementedError("Attempting age diff id twice")
                        match_with_age_diff[j] = df_matches.loc[idx]

                    df_test = df_test.drop(index=idx)
                    mpv_matched[j] = True

        
        for j, row_match in mpv_agency.iterrows():
            if len(df_test)==0:
                break
            if mpv_matched[j]:
                continue
            # Look for matches where dates differ
            is_match, _, _ = ois_matching.check_for_match(df_test, row_match)

            if is_match.sum()>0:
                df_matches = df_test[is_match]
                if len(df_matches)>1:
                    date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '3d')
                    if addr_col:
                        addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[addr_col], notfound='error')

                    if date_close.sum()==1 and (not addr_col or addr_match[date_close].iloc[0]):
                        df_test = df_test.drop(index=df_matches[date_close].index)
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
                        df_test = df_test.drop(index=df_matches.index)
                        mpv_matched[j] = True
                    elif ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '11d').iloc[0]:
                        if include_close_date_matching_zip and \
                            ois_matching.zipcode_isequal(row_match, df_matches, iloc2=0):
                            # row_match[zip_col]==df_matches[zip_col].iloc[0]:
                            df_test = df_test.drop(index=df_matches.index)
                            mpv_matched[j] = True
                        elif include_close_date_matching_zip and \
                            ois_matching.zipcode_isequal(row_match, df_matches, iloc2=0, count='none'):
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
                    date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '2d').iloc[0]
                    addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[addr_col], notfound='error').iloc[0]
                    
                    if date_close and addr_match:
                        df_test = df_test.drop(index=df_test[is_match].index)
                        mpv_matched[j] = True
                    elif addr_match and ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '31d', '30d').iloc[0]:
                        # Likely error in the month that was recorded
                        df_test = df_test.drop(index=df_test[is_match].index)
                        mpv_matched[j] = True
                    elif addr_match:
                        raise NotImplementedError()
                    elif ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '110d').iloc[0]:
                        raise NotImplementedError()

        j = 0
        while j<len(df_test):
            if len(mpv_agency)==0:
                break
            
            mpv_unmatched = mpv_agency[~mpv_matched]
            if not addr_col:
                date_close = ois_matching.in_date_range(df_test.iloc[j][date_col], mpv_unmatched[date_col], '5d')
                if not date_close.any():
                    j+=1
                    continue
                if isinstance(df_test.iloc[j][date_col], pd.Period):
                    j+=1
                    continue
                
                date_diff = abs(mpv_unmatched[date_close][date_col] - df_test.iloc[j][date_col])
                if zip_col in mpv_unmatched and zip_col in df_test:
                    if not (zip_matches:=mpv_unmatched[date_close][zip_col]==df_test.iloc[j][zip_col]).any():
                        j+=1  # No zip codes match
                        continue
                    if (m:=(date_diff[zip_matches]<='4d')).any():  # Zip codes do match
                        is_match, _, _ = ois_matching.check_for_match(
                            mpv_unmatched[date_close][zip_matches][m], df_test.iloc[j], 
                            max_age_diff=5, allowed_replacements=allowed_replacements)
                        if is_match.sum()==1:
                            match_with_age_diff[is_match[is_match].index[0]] = df_test.iloc[j]
                            df_test = df_test.drop(index=df_test.index[j])
                            mpv_matched[is_match[is_match].index[0]] = True
                            continue
                        elif test_gender_col in df_test and df_test.iloc[j][test_gender_col]=='FEMALE' and \
                            (mpv_unmatched[date_close][mpv_gender_col]=="MALE").all():
                            j+=1
                            continue

                raise NotImplementedError()

            matches = ois_matching.street_match(df_test.iloc[j][addr_col], addr_col, mpv_unmatched[mpv_addr], notfound='error')

            if matches.any():
                date_close = ois_matching.in_date_range(df_test.iloc[j][date_col], mpv_unmatched[matches][date_col], '3d')
                if date_close.any():
                    if date_close.sum()>1:
                        raise NotImplementedError()
                    date_close = [k for k,x in date_close.items() if x][0]
                    # Consider this a match with errors in the demographics
                    if date_close in subject_demo_correction:
                        raise NotImplementedError("Attempting demo correction twice")
                    subject_demo_correction[date_close] = df_test.iloc[j]
                    df_test = df_test.drop(index=df_test.index[j])
                    mpv_matched[date_close] = True
                elif (ois_matching.in_date_range(df_test.iloc[j][date_col], mpv_unmatched[matches][date_col], '150d')).any() and \
                    (len(mpv_unmatched[matches])>1 or address_parser.tag(mpv_unmatched[mpv_addr][matches].iloc[0], mpv_addr)[1]!='Coordinates'):
                    match_sum = \
                        (mpv_unmatched[matches>0][mpv_race_col] == df_test.iloc[j][test_race_col]).apply(lambda x: 1 if x else 0) + \
                        (mpv_unmatched[matches>0][mpv_age_col] == df_test.iloc[j][test_age_col]).apply(lambda x: 1 if x else 0) + \
                        (mpv_unmatched[matches>0][mpv_gender_col] == df_test.iloc[j][test_gender_col]).apply(lambda x: 1 if x else 0)
                    if mpv_unmatched[matches>0]['officer_names'].notnull().any():
                        raise NotImplementedError("Check this")
                    elif (abs(df_test.iloc[j][date_col]-mpv_unmatched[matches>0][date_col])<'30d').any():
                        raise NotImplementedError("Check this")
                    elif (match_sum==3).any():
                        raise NotImplementedError("Check this")

            j+=1

        df_save = []
        if len(df_test)>0:
            df_test['type'] = 'Unmatched'
            df_save.append(df_test)
        if len(subject_demo_correction)>0:
            df = pd.DataFrame(subject_demo_correction).transpose()
            df['type'] = 'Demo Correction?'
            # Create hash of MPV row
            df['MPV Hash'] = mpv_agency.loc[df.index].apply(
                lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
                )

            df["MPV Row"] = df.index
            df["MPV ID"] = mpv_agency.loc[df.index]['mpv_id']
            df["MPV DATE"] = mpv_agency.loc[df.index][date_col]
            df["MPV RACE"] = mpv_agency.loc[df.index][mpv_race_col]
            df["MPV GENDER"] = mpv_agency.loc[df.index][mpv_gender_col]
            df["MPV AGE"] = mpv_agency.loc[df.index][mpv_age_col]
            df["MPV AGENCY"] = mpv_agency.loc[df.index][agency_col]
            df["MPV ADDRESS"] = mpv_agency.loc[df.index][mpv_addr]
            df_save.append(df)
        
        if len(match_with_age_diff)>0:
            df = pd.DataFrame(match_with_age_diff).transpose()
            df['type'] = 'Age Difference'
            # Create hash of MPV row
            df['MPV Hash'] = mpv_agency.loc[df.index].apply(
                lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
                )

            df["MPV Row"] = df.index
            df["MPV ID"] = mpv_agency.loc[df.index]['mpv_id']
            df["MPV DATE"] = mpv_agency.loc[df.index][date_col]
            df["MPV RACE"] = mpv_agency.loc[df.index][mpv_race_col]
            df["MPV GENDER"] = mpv_agency.loc[df.index][mpv_gender_col]
            df["MPV AGE"] = mpv_agency.loc[df.index][mpv_age_col]
            df["MPV AGENCY"] = mpv_agency.loc[df.index][agency_col]
            df["MPV ADDRESS"] = mpv_agency.loc[df.index][mpv_addr]
            df_save.append(df)

        if len(df_save)>0:
            df_save = pd.concat(df_save, ignore_index=True)
            df_save['MPV Download Date'] = mpv_download_date
            df_save['Agency'] = agency
            df_save['known_fatal'] = known_fatal

            keys = ["MPV ID", 'type', 'known_fatal', 'Agency', date_col]
            if test_race_col  in df_save:
                keys.append(test_race_col )
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
            source_basename = f"{row['SourceName']}_{row['State']}_{row['TableType']}_{row['Year']}"
            opd_logger.log(df_save, output_dir, source_basename, keys=keys, add_date=True, only_diffs=True)

            cols = ['type', 'known_fatal']
            for c in df_save.columns:
                if c.startswith("MPV"):
                    cols.append(c)

            df_global = df_save[cols].copy()
            df_global["OPD Date"] = df_save[date_col]
            df_global["OPD Agency"] = df_save['Agency']

            if test_race_col  in df_save:
                df_global["OPD Race"] = df_save[test_race_col ]
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
