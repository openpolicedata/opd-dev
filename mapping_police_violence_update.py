import pandas as pd
import numpy as np
import os, sys
import glob
from hashlib import sha1
from datetime import datetime
file_loc = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_loc)  # Add current file directory to path
from opddev.utils import address_parser
from opddev.utils import ois_matching
from opddev.utils.ois_matching import date_col, race_col, agency_col, fatal_col, gender_col, age_col, injury_cols
import openpolicedata as opd
import logging

istart = 0
logging_level = logging.DEBUG
include_unknown_fatal = True

opd.datasets.reload(r"..\opd-data\opd_source_table.csv")

mpv_folder = os.path.join(file_loc, r"data\MappingPoliceViolence")
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
mpv = opd.data.Table({"SourceName":"Mapping Policing Violence", 
                      "State":opd.defs.MULTI, 
                      "TableType":opd.defs.TableType.SHOOTINGS}, 
                     mpv_raw,
                     opd.defs.MULTI)
mpv.standardize(known_cols={agency_col:"agency_responsible"})
mpv.expand()
mpv = mpv.table

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
    logger.debug(f'Running {k} of {len(df_ois)}: {row["SourceName"]} {row["TableType"]}')
    src = opd.Source(row["SourceName"], state=row["State"])    # Create source for agency
    if row['TableType']==opd.defs.TableType.SHOOTINGS_INCIDENTS:
        raise NotImplementedError("Need to handle data with information spread across multiple tables")
    
    t = src.load_from_url(row['Year'], row['TableType'])  # Load data
    t.standardize(agg_race_cat=True)
    t.expand(mismatch='splitsingle')
    df_test = t.table

    df_test = ois_matching.remove_officer_rows(df_test)

    is_multi_subject = False
    known_fatal = True
    if fatal_col in df_test:
        fatal_values = ['YES',"UNSPECIFIED",'SELF-INFLICTED FATAL'] if include_unknown_fatal else ['YES','SELF-INFLICTED FATAL']
        df_test = df_test[df_test[fatal_col].isin(fatal_values)]
    elif len(c:=[x for x in injury_cols if x in df_test])>0:
        df_test = df_test[df_test[c[0]]=='FATAL']
    else:
        if not include_unknown_fatal:
            continue
        known_fatal = False

    if date_col not in df_test:
        raise NotImplementedError("Need to handle data without date")
    
    df_test = ois_matching.filter_by_date(df_test.copy(), date_col, min_date)

    df_test['OPD Hash'] = df_test.apply(
            lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
            )

    if agency_col in [x.new_column_name for x in t.get_transform_map()]:
        raise NotImplementedError("Need to handle state-level multi-agency data data")
    
    agency_matches = (mpv[agency_col].str.lower().str.contains(row["SourceName"].lower()).fillna(False)) & \
         (mpv['state']==opd.defs.states[row['State']])
    if agency_matches.sum()==0:
        logger.info(f"No MPV shootings found for {row['SourceName']}")
        
    mpv_agency = mpv[agency_matches]

    agency_words = ['Area','Rapid','Transit', 'Police', 'Department','Crisis', "Sheriff", 
                    'Township', 'Bureau', 'State', 'University', 'Public', 'Safety',
                    'Housing']
    agency_types = ['Area Rapid Transit Police Department','Police Department', 'Crisis Response Team', "Sheriff's Office", 
                    'Township Police Department',
                    'Police Bureau', 'State University Department of Public Safety',
                    'Housing Authority Police Department','Marshal Service']
    for j, mpv_row in mpv_agency.iterrows():
        if mpv_row[agency_col] != row['AgencyFull'] and \
            mpv_row[agency_col] not in [row['Agency']+" "+x for x in agency_types] and \
            mpv_row[agency_col] != f"{row['Agency']} {opd.defs.states[row['State']]} Police Department":
        
            # Agency can be a comma-separated list of multiple agencies
            agencies_check = mpv_row[agency_col]
            agencies_check = agencies_check[1:] if agencies_check[0]=='"' else agencies_check
            agencies_check = agencies_check[:-1] if agencies_check[-1]=='"' else agencies_check
            agencies_check = [y.strip() for y in agencies_check.split(',')]
            for a in agencies_check:
                if a.lower()==row['AgencyFull'].lower() or (a.startswith(row['Agency']+" ") and \
                    any([a.replace(row['Agency']+" ","").startswith(x) for x in agency_words])):
                    break
                type_match = [x for x in agency_types if a.endswith(x)]
                stem = a.replace(type_match[0],"").strip()
                type_match_opd = [x for x in agency_types if row['AgencyFull'].endswith(x)]
                if not (type_match[0]==type_match_opd[0] or stem==row['Agency']) or \
                    a.endswith(row['AgencyFull']):
                    break
            else:
                raise NotImplementedError(f"{row['AgencyFull']} not found in {mpv_row[agency_col]}")
            
    test_cols, ignore_cols = ois_matching.columns_for_duplicated_check(t, df_test)
    df_test = ois_matching.drop_duplicates(df_test, subset=test_cols)

    addr_col = address_parser.find_address_col(df_test)
    if len(addr_col)==1:
        addr_col = addr_col[0]
        address_found = True
    else:
        address_found = False

    subject_demo_correction = {}
    match_with_age_diff = {}
    mpv_matched = pd.Series(False, mpv_agency.index)
    for j, row_match in mpv_agency.iterrows():
        df_matches = ois_matching.find_date_matches(df_test, date_col, row_match[date_col])

        if len(df_matches)==0:
            continue

        # Run ois_matching.check_for_match multiple times. Loosen requirements for a match each time
        args = [{}, {'max_age_diff':1,'check_race_only':True}, 
                {'allowed_replacements':{race_col:[["HISPANIC/LATINO","INDIGENOUS"],["HISPANIC/LATINO","WHITE"]]}},
                {'inexact_age':True}, {'max_age_diff':5}, {'allow_race_diff':True}]
        age_diff = pd.Series(False, index=df_matches.index)
        for a in args:
            is_match, is_unknown, num_matches, is_diff_race = ois_matching.check_for_match(df_matches, row_match, **a)
            if is_match.any():
                age_diff[is_match] = list(a.keys())==['max_age_diff']
                break

        if is_match.sum()==0:
            if len(df_matches)==1:
                # if j in subject_demo_correction:
                #     raise NotImplementedError("Attempting demo correction twice")
                # subject_demo_correction[j] = df_matches.iloc[0]
                # df_test = df_test.drop(index=df_matches.index[0])
                # mpv_matched[j] = True
                logger.warning(f"Match found for {row_match[date_col]} but demographics do not match")
                continue
            
            raise NotImplementedError()

        if is_match.sum()>1:
            throw = True
            summary_col = [x for x in df_matches.columns if 'summary' in x.lower()]
            if address_found:
                test_cols_reduced = test_cols.copy()
                test_cols_reduced.remove(addr_col)
                [test_cols_reduced.remove(x) for x in summary_col]
                if len(ois_matching.drop_duplicates(df_matches[is_match], subset=test_cols_reduced, ignore_null=True, ignore_date_errors=True))==1:
                    # These are the same except for the address. Check if the addresses are similar
                    addr_match = ois_matching.street_match(df_matches[is_match][addr_col].iloc[0], addr_col, 
                                                           df_matches[is_match][addr_col].iloc[1:], notfound='error')
                    throw = not addr_match.all()
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
        is_match, _, _, _ = ois_matching.check_for_match(df_test, row_match)

        if is_match.sum()>0:
            df_matches = df_test[is_match]
            if len(df_matches)>1:
                date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '1d')
                addr_match = ois_matching.street_match(row_match[mpv_addr], mpv_addr, df_matches[addr_col], notfound='error')

                if date_close.sum()==1 and (addr_match[date_close].iloc[0] or not address_found):
                    df_test = df_test.drop(index=df_matches[date_close].index)
                    mpv_matched[j] = True
                elif not addr_match.any() or \
                    ois_matching.in_date_range(df_matches[addr_match][date_col], row_match[date_col], min_delta='300d').all():
                    continue
                else:
                    raise NotImplementedError()
            elif not address_found and ois_matching.in_date_range(df_matches[date_col],row_match[date_col], min_delta='50d').iloc[0]:
                continue
            else:
                date_close = ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '1d').iloc[0]
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
                elif ois_matching.in_date_range(df_matches[date_col], row_match[date_col], '300d').iloc[0]:
                    raise NotImplementedError()

    j = 0
    while j<len(df_test):
        if len(mpv_agency)==0:
            break
        
        mpv_unmatched = mpv_agency[~mpv_matched]
        if not address_found and not ois_matching.in_date_range(df_test.iloc[j][date_col], mpv_unmatched[date_col], '20d').any():
            j+=1
            continue
        elif not address_found and isinstance(df_test.iloc[j][date_col], pd.Period):
            j+=1
            continue
        elif not address_found:
            raise NotImplementedError()

        matches = ois_matching.street_match(df_test.iloc[j][addr_col], addr_col, mpv_unmatched[mpv_addr], notfound='error')

        if (matches>0).any():
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
            elif (abs(df_test.iloc[j][date_col]-mpv_unmatched[matches>0][date_col])<='300d').any():
                rcol = ois_matching.get_opd_race_col(df_test)
                gcol = ois_matching.get_opd_gender_col(df_test)
                acol = ois_matching.get_opd_age_col(df_test)
                match_sum = (mpv_unmatched[matches>0][race_col] == df_test.iloc[j][rcol]).apply(lambda x: 1 if x else 0) + \
                    (mpv_unmatched[matches>0][age_col] == df_test.iloc[j][acol]).apply(lambda x: 1 if x else 0) + \
                    (mpv_unmatched[matches>0][gender_col] == df_test.iloc[j][gcol]).apply(lambda x: 1 if x else 0)
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
        df_test['known_fatal'] = known_fatal
        df_save.append(df_test)
    if len(subject_demo_correction)>0:
        df = pd.DataFrame(subject_demo_correction).transpose()
        df['type'] = 'Demo Correction?'
        # Create hash of MPV row
        df['MPV Hash'] = mpv_agency.loc[df.index].apply(
            lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
            )

        df["MPV Row"] = df.index
        df["mpv_id"] = mpv_agency.loc[df.index]['mpv_id']
        df["MPV "+date_col] = mpv_agency.loc[df.index][date_col]
        df["MPV "+race_col] = mpv_agency.loc[df.index][race_col]
        df["MPV "+gender_col] = mpv_agency.loc[df.index][gender_col]
        df["MPV "+age_col] = mpv_agency.loc[df.index][age_col]
        df["MPV "+agency_col] = mpv_agency.loc[df.index][agency_col]
        df["MPV "+mpv_addr] = mpv_agency.loc[df.index][mpv_addr]
        df_save.append(df)
    
    if len(match_with_age_diff)>0:
        df = pd.DataFrame(match_with_age_diff).transpose()
        df['type'] = 'Age Difference'
        # Create hash of MPV row
        df['MPV Hash'] = mpv_agency.loc[df.index].apply(
            lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
            )

        df["MPV Row"] = df.index
        df["mpv_id"] = mpv_agency.loc[df.index]['mpv_id']
        df["MPV "+date_col] = mpv_agency.loc[df.index][date_col]
        df["MPV "+race_col] = mpv_agency.loc[df.index][race_col]
        df["MPV "+gender_col] = mpv_agency.loc[df.index][gender_col]
        df["MPV "+age_col] = mpv_agency.loc[df.index][age_col]
        df["MPV "+agency_col] = mpv_agency.loc[df.index][agency_col]
        df["MPV "+mpv_addr] = mpv_agency.loc[df.index][mpv_addr]
        df_save.append(df)

    if len(df_save)>0:
        df_save = pd.concat(df_save, ignore_index=True)
        df_save['MPV Download Date'] = mpv_download_date

        mpv_cols = [x for x in df_save.columns if x.lower().startswith("mpv") or x=='known_fatal']
        new_cols = ['type']
        new_cols.extend(mpv_cols)
        new_cols.extend([x for x in df_save.columns if x not in new_cols])
        df_save = df_save[new_cols]

        out_filename = f"{row['SourceName']}_{row['State']}_{row['TableType']}_{row['Year']}.csv"
        out_filename = os.path.join(mpv_folder, "Updates", out_filename)

        old_files = glob.glob(out_filename.replace(".csv","*"))
        for f in old_files:
            df_old = pd.read_csv(f, keep_default_na=False,
                                 na_values={'',np.nan})#.convert_dtypes()
            if 'MPV DATE' in df_old:
                df_old['MPV DATE'] = pd.to_datetime(df_old['MPV DATE'])
            if df_save[date_col].apply(lambda x: isinstance(x,pd.Period)).any():
                if df_save[date_col][0].freq == 'Y':
                    df_old[date_col] = pd.to_datetime(df_old[date_col], format='%Y')
                else:
                    raise NotImplementedError()
                df_old[date_col] = df_old[date_col].apply(lambda x: pd.Period(x,df_save[date_col][0].freq))
            else:
                df_old[date_col] = pd.to_datetime(df_old[date_col])
            if 'TIME' in df_old:
                df_old['TIME'] = pd.to_datetime(df_old['TIME'],format= '%H:%M:%S' ).dt.time
                df_old['DATETIME'] = pd.to_datetime(df_old['DATETIME'])
            try:
                df_old['MPV Download Date'] = pd.to_datetime(df_old['MPV Download Date'],format=r'%Y%m%d')
            except:
                df_old['MPV Download Date'] = pd.to_datetime(df_old['MPV Download Date'])                
            assert len(df_old)==len(df_save)
            is_equal = True
            df_old = df_old.sort_values(by=date_col, ignore_index=True)
            df_save = df_save.sort_values(by=date_col, ignore_index=True)
            for c in df_save.columns:
                if c not in df_old or 'objectid' in c.lower() or c in ['OPD Hash']:
                    continue
                if not df_old[c].equals(df_save[c]) and \
                    not ((df_old[c]==df_save[c]) | (df_old[c].isnull() & df_save[c].isnull())).all() and \
                     not (df_old[c].apply(str)==df_save[c].apply(str)).all():
                    raise NotImplementedError("Check this!")

            os.remove(f)

        df_save.to_csv(out_filename, index=False)

        out_filename = os.path.join(mpv_folder, "Updates", 'Potential_MPV_Updates_Global.csv')

        df_save['OPD Agency'] = row['AgencyFull']
        first_cols = ['type', 'mpv_id', 'MPV Hash', 'MPV Row', 'MPV DATE', 'MPV SUBJECT_RE_GROUP', 'MPV SUBJECT_GENDER',
                'MPV SUBJECT_AGE', 'MPV street_address', 'MPV AGENCY', 'MPV Download Date', 'OPD Agency', 'OPD Hash']
        first_cols = [x for x in first_cols if x in df_save]
        if date_col in df_save:
            first_cols.append(date_col)

        if ois_matching.get_opd_race_col(df_save) in df_save:
            first_cols.append(ois_matching.get_opd_race_col(df_save))
        if ois_matching.get_opd_gender_col(df_save) in df_save:
            first_cols.append(ois_matching.get_opd_gender_col(df_save))
        if ois_matching.get_opd_age_col(df_save) in df_save:
            first_cols.append(ois_matching.get_opd_age_col(df_save))
        if address_found:
            first_cols.append(addr_col)

        if os.path.exists(out_filename):
            df = pd.read_csv(out_filename)
            if (df['OPD Agency']==row['AgencyFull']).any():
                df_check = df[df['OPD Agency']==row['AgencyFull']]
                if df_save['OPD Hash'].isin(df_check['OPD Hash']).all():
                    continue
                else:
                    raise NotImplementedError()
            else:
                df = pd.concat([df, df_save[first_cols]], ignore_index=True)
        else:
            df = df_save[first_cols]

        df.to_csv(out_filename, index=False)
