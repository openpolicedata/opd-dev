import pandas as pd
import os, sys
from hashlib import sha1
from datetime import datetime
file_loc = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(file_loc)  # Add current file directory to path
from opddev.utils import address_parser
from opddev.utils import ois_matching
from opddev.utils.ois_matching import date_col, race_col, agency_col, fatal_col, gender_col, age_col, injury_cols
import openpolicedata as opd
import logging

logging_level = logging.DEBUG

opd.datasets.reload(r"..\opd-data\opd_source_table.csv")

istart = 8

mpv_addr = "street_address"
mpv_folder = os.path.join(file_loc, r"data\MappingPoliceViolence")
mpv_csv_filename = "Mapping Police Violence_Accessed20231111.csv"
mpv_download_date = mpv_csv_filename[-4-8:-4]
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
    t.expand()
    df_test = t.table

    df_test = ois_matching.remove_officer_rows(df_test)

    is_multi_subject = False
    if fatal_col in df_test:
        df_test = df_test[df_test[fatal_col].isin(['YES',"UNSPECIFIED",'SELF-INFLICTED FATAL'])]
    elif len(c:=[x for x in injury_cols if x in df_test])>0:
        df_test = df_test[df_test[c[0]]=='FATAL']
    else:
        raise NotImplementedError("Need to handle data without fatal or injury column")

    if date_col not in df_test:
        raise NotImplementedError("Need to handle data without date")
    
    df_test = df_test[df_test[date_col] >= min_date]

    if agency_col in [x.new_column_name for x in t.get_transform_map()]:
        raise NotImplementedError("Need to handle state-level multi-agency data data")
    
    agency_matches = (mpv[agency_col].str.lower().str.contains(row["SourceName"].lower()).fillna(False)) & \
         (mpv['state']==opd.defs.states[row['State']])
    if agency_matches.sum()==0:
        logger.info(f"No MPV shootings found for {row['SourceName']}")
        
    mpv_agency = mpv[agency_matches]

    agency_types = ['Police Department', 'Crisis Response Team', "Sheriff's Office", 'Township Police Department']
    for j, mpv_row in mpv_agency.iterrows():
        if mpv_row[agency_col] != row['AgencyFull'] and \
            mpv_row[agency_col] not in [row['Agency']+" "+x for x in agency_types] and \
            mpv_row[agency_col] != f"{row['Agency']} {opd.defs.states[row['State']]} Police Department":
        
            if "," in mpv_row[agency_col]:
                # Agency can be a comma-separated list of multiple agencies
                x = mpv_row[agency_col]
                x = x[1:] if x[0]=='"' else x
                x = x[:-1] if x[-1]=='"' else x
                xlist = [y.strip() for y in x.split(',')]
                if row['AgencyFull'] not in xlist:
                    raise NotImplementedError(f"{row['AgencyFull']} not found in {x}")
            else:
                type_match = [x for x in agency_types if mpv_row[agency_col].endswith(x)]
                stem = mpv_row[agency_col].replace(type_match[0],"").strip()
                type_match_opd = [x for x in agency_types if row['AgencyFull'].endswith(x)]
                if type_match[0]==type_match_opd[0] or stem==row['Agency']:
                    raise NotImplementedError()
            
    test_cols, ignore_cols = ois_matching.columns_for_duplicated_check(t, df_test)
    df_test = df_test.drop_duplicates(subset=test_cols, ignore_index=True)
    subject_demo_correction = {}
    match_with_age_diff = {}
    mpv_matched = pd.Series(False, mpv_agency.index)
    for j, row_match in mpv_agency.iterrows():
        # MPV's date column does not have times so zero out time
        dates_test = df_test[date_col].apply(lambda x: x.replace(hour=0, minute=0, second=0))
        df_matches = df_test[dates_test == row_match[date_col]]

        if len(df_matches)==0:
            continue

        # Run ois_matching.check_for_match multiple times. Loosen requirements for a match each time
        args = [{}, {'max_age_diff':1,'check_race_only':True}, 
                {'allowed_replacements':{race_col:[["HISPANIC/LATINO","INDIGENOUS"],["HISPANIC/LATINO","WHITE"]]}},
                {'inexact_age':True}, {'max_age_diff':5}, {'allow_race_diff':True}]
        age_diff = False
        for a in args:
            is_match, is_unknown, num_matches, is_diff_race = ois_matching.check_for_match(df_matches, row_match, **a)
            if is_match.sum()>0:
                age_diff = list(a.keys())==['max_age_diff']
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
            raise NotImplementedError("Multiple matches found")
        
        if (num_matches[is_match]<1).all():
            raise NotImplementedError("Number of matches error")
                
        for idx in df_matches.index:
            # OIS found in data. Remove from df_test.
            if is_match[idx]: 
                if is_unknown[idx] or is_diff_race[idx]:
                    if j in subject_demo_correction:
                        raise NotImplementedError("Attempting demo correction twice")
                    subject_demo_correction[j] = df_matches.loc[idx]
                if age_diff:
                    if j in match_with_age_diff:
                        raise NotImplementedError("Attempting age diff id twice")
                    match_with_age_diff[j] = df_matches.loc[idx]

                df_test = df_test.drop(index=idx)
                mpv_matched[j] = True

    addr_col = ois_matching.find_address_col(df_test)
    if len(addr_col)==1:
        addr_col = addr_col[0]
    elif len(df_test)>0:
        raise NotImplementedError()
    
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
                raise NotImplementedError()
            else:
                date_close = (abs(df_matches[date_col]-row_match[date_col])<='1d').iloc[0]
                addr, addr_type = address_parser.tag(row_match[mpv_addr], mpv_addr)

                addr_match = False
                streetname_found = False
                for k,v in addr.items():
                    if k.endswith('StreetName'):
                        streetname_found = True
                        if v.lower() in df_matches[addr_col].iloc[0].lower():
                            addr_match = True
                            break
                assert streetname_found
                
                if date_close and addr_match:
                    df_test = df_test.drop(index=df_test[is_match].index)
                    mpv_matched[j] = True
                elif addr_match:
                    raise NotImplementedError()
                elif (abs(df_matches[date_col]-row_match[date_col])<'300d').iloc[0]:
                    raise NotImplementedError()

    j = 0
    while j<len(df_test):
        if len(mpv_agency)==0:
            break
        addr, addr_type = address_parser.tag(df_test.iloc[j][addr_col], addr_col)
        
        mpv_unmatched = mpv_agency[~mpv_matched]
        matches = pd.Series(0, index=mpv_unmatched.index)
        tot = 0
        for k,v in addr.items():
            if k.endswith('StreetName'):
                tot+=1
                matches = matches + mpv_unmatched[mpv_addr].str.lower().str.contains(v.lower())
        assert tot>0

        if (matches>0).any():
            date_close = (abs(df_test.iloc[j][date_col]-mpv_unmatched[matches>0][date_col])<='1d')
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
        df_save.append(df_test)
    if len(subject_demo_correction)>0:
        df = pd.DataFrame(subject_demo_correction).transpose()
        df['type'] = 'Demo Correction?'
        # Create hash of MPV row
        df['MPV Hash'] = mpv_agency.loc[df.index].apply(
            lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
            )

        df["MPV Row"] = df.index
        df["MPV "+date_col] = mpv_agency.loc[df.index][date_col]
        df["MPV "+race_col] = mpv_agency.loc[df.index][race_col]
        df["MPV "+gender_col] = mpv_agency.loc[df.index][gender_col]
        df["MPV "+age_col] = mpv_agency.loc[df.index][age_col]
        df["MPV "+agency_col] = mpv_agency.loc[df.index][agency_col]
        df_save.append(df)
    
    if len(match_with_age_diff)>0:
        df = pd.DataFrame(match_with_age_diff).transpose()
        df['type'] = 'Age Difference'
        # Create hash of MPV row
        df['MPV Hash'] = mpv_agency.loc[df.index].apply(
            lambda x: ''.join(x.astype(str)),axis=1).apply(lambda value: sha1(str(value).encode('utf-8')).hexdigest()
            )

        df["MPV Row"] = df.index
        df["MPV "+date_col] = mpv_agency.loc[df.index][date_col]
        df["MPV "+race_col] = mpv_agency.loc[df.index][race_col]
        df["MPV "+gender_col] = mpv_agency.loc[df.index][gender_col]
        df["MPV "+age_col] = mpv_agency.loc[df.index][age_col]
        df["MPV "+agency_col] = mpv_agency.loc[df.index][agency_col]
        df_save.append(df)

    if len(df_save)>0:
        out_filename = f"{row['SourceName']}_{row['State']}_{row['TableType']}_{row['Year']}_{datetime.now().strftime('%Y%m')}.csv"
        out_filename = os.path.join(mpv_folder, "Updates", out_filename)
        df_save = pd.concat(df_save, ignore_index=True)
        df_save['MPV Download Date'] = mpv_download_date

        mpv_cols = [x for x in df_save.columns if x.startswith("MPV ")]
        new_cols = ['type']
        new_cols.extend(mpv_cols)
        new_cols.extend([x for x in df_save.columns if x not in new_cols])
        df_save = df_save[new_cols]

        if os.path.exists(out_filename):
            old_df = pd.read_csv(out_filename)
            assert len(old_df)==len(df_save)
            os.remove(out_filename)

        df_save.to_csv(out_filename, index=False)
