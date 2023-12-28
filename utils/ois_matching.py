import math
import numbers
import openpolicedata as opd
import pandas as pd
import re
from . import address_parser

# Columns will be standardized below to use these names
date_col = opd.defs.columns.DATE
race_col = opd.defs.columns.RE_GROUP_SUBJECT
gender_col = opd.defs.columns.GENDER_SUBJECT
age_col = opd.defs.columns.AGE_SUBJECT
agency_col = opd.defs.columns.AGENCY
fatal_col = opd.defs.columns.FATAL_SUBJECT
role_col = opd.defs.columns.SUBJECT_OR_OFFICER
injury_cols = [opd.defs.columns.INJURY_SUBJECT, opd.defs.columns.INJURY_OFFICER_SUBJECT]

# Race values will be standardized below to use these values
race_vals = opd.defs.get_race_cats()
# Only keep specific values for testing later
[race_vals.pop(k) for k in ["MULTIPLE","OTHER","OTHER / UNKNOWN", "UNKNOWN", "UNSPECIFIED"]]
race_vals = race_vals.values()

# Gender values will be standardized below to use these values
gender_vals = opd.defs.get_gender_cats()
# Only keep specific values for testing later
[gender_vals.pop(k) for k in ["MULTIPLE","OTHER","OTHER / UNKNOWN", "UNKNOWN", "UNSPECIFIED"] if k in gender_vals]
gender_vals = gender_vals.values()


def split_words(string, case=None):
    # Split based on spaces, punctuation, and camel case
    words = list(re.split(r"[^A-Za-z\d]+", string))
    k = 0
    while k < len(words):
        if len(words[k])==0:
            del words[k]
            continue
        new_words = opd.utils.camel_case_split(words[k])
        words[k] = new_words[0]
        for j in range(1, len(new_words)):
            words.insert(k+1, new_words[j])
            k+=1
        k+=1

    if case!=None:
        if case.lower()=='lower':
            words = [x.lower() for x in words]
        elif case.lower()=='upper':
            words = [x.upper() for x in words]
        else:
            raise ValueError("Unknown input case")

    return words


def drop_duplicates(df, subset=None, ignore_null=False, ignore_date_errors=False):
    subset = subset if subset else df.columns
    if opd.defs.columns.DATE in df and not isinstance(df[opd.defs.columns.DATE].dtype, pd.PeriodDtype):
        df = df.copy()
        df[opd.defs.columns.DATE] = df[opd.defs.columns.DATE].apply(lambda x: x.replace(hour=0, minute=0, second=0))

    try:
        df = df.drop_duplicates(subset=subset, ignore_index=True)
        df_mod = df.copy()[subset]
    except TypeError as e:
        if len(e.args)>0 and e.args[0]=="unhashable type: 'dict'":
            df_mod = df.copy()
            df_mod = df_mod.apply(lambda x: x.apply(lambda y: str(y) if isinstance(y,dict) else y))
            dups = df_mod.duplicated(subset=subset)
            df = df[~dups].reset_index()
            df_mod = df_mod[~dups].reset_index()[subset]
        else:
            raise
    except:
        raise

    # Attempt cleanup and try again
    p = re.compile('\s?&\s?')
    df_mod = df_mod.apply(lambda x: x.apply(lambda x: p.sub(' and ',x).lower() if isinstance(x,str) else x))

    if ignore_date_errors:
        # Assume that if the full date matches, differences in other date-related fields should 
        # not block this from being a duplicate
        if any([x for x in subset if 'date' in x.lower()]):
            partial_date_terms = ['month','year','day','hour']
            reduced_subset = [x for x in subset if not any([y in x.lower() for y in partial_date_terms])]
            df_mod = df_mod[reduced_subset]

    dups = df_mod.duplicated()

    if ignore_null:
        # Assume that there are possibly multiple entries where some do no include all the information
        df_mod = df_mod.replace(opd.defs.UNSPECIFIED.lower(), pd.NA)
        for j in df_mod.index:
            for k in df_mod.index[j+1:]:
                if dups[j]:
                    break
                if dups[k]:
                    continue
                rows = df_mod.loc[[j,k]]
                nulls = rows.isnull().sum(axis=1)
                rows = rows.dropna(axis=1)
                if rows.duplicated(keep=False).all():
                    dups[nulls.idxmax()] = True

    df = df[~dups]

    return df


def in_date_range(dt1, dt2, max_delta=None, min_delta=None):
    count1 = count2 = 1
    if is_series1:=isinstance(dt1, pd.Series):
        count1 = len(dt1)
        if is_timestamp1:=not isinstance(dt1.dtype, pd.api.types.PeriodDtype):
            dt1 = dt1.dt.tz_localize(None)
    elif is_timestamp1:=isinstance(dt1, pd.Timestamp):
        dt1 = dt1.tz_localize(None)

    if is_series2:=isinstance(dt2, pd.Series):
        count2 = len(dt2)
        if is_timestamp2:=not isinstance(dt2.dtype, pd.api.types.PeriodDtype):
            dt2 = dt2.dt.tz_localize(None)
    elif is_timestamp2:=isinstance(dt2, pd.Timestamp):
        dt2 = dt2.tz_localize(None)

    if count1!=count2 and not (count1==1 or count2==1):
        raise ValueError("Date inputs are different sizes")
    
    if isinstance(dt1, pd.Series) and count2==1:
        matches = pd.Series(True, index=dt1.index)
    elif isinstance(dt2, pd.Series):
        matches = pd.Series(True, index=dt2.index)
    else:
        matches = True

    if max_delta:
        if not is_timestamp1 and not is_timestamp2:
            raise NotImplementedError()
        elif not is_series2 and not is_timestamp2:
            matches = matches & (((dt2.end_time >= dt1) & (dt2.start_time <= dt1)) | \
                ((dt2.end_time - dt1).abs()<=max_delta) | ((dt2.start_time - dt1).abs()<=max_delta))
        elif not is_series1 and not is_timestamp1:
            matches = matches & (((dt1.end_time >= dt2) & (dt1.start_time <= dt2)) | \
                ((dt1.end_time - dt2).abs()<=max_delta) | ((dt1.start_time - dt2).abs()<=max_delta))
        elif is_series2 and not is_timestamp2:
            matches = matches & (((dt2.dt.end_time >= dt1) & (dt2.dt.start_time <= dt1)) | \
                ((dt2.dt.end_time - dt1).abs()<=max_delta) | ((dt2.dt.start_time - dt1).abs()<=max_delta))
        elif is_series1 and not is_timestamp1:
            matches = matches & (((dt1.dt.end_time >= dt2) & (dt1.dt.start_time <= dt2)) | \
                ((dt1.dt.end_time - dt2).abs()<=max_delta) | ((dt1.dt.start_time - dt2).abs()<=max_delta))
        else:
            matches = matches & ((dt1 - dt2).abs()<=max_delta)
        
    if min_delta:
        if not is_timestamp1 and not is_timestamp2:
            raise NotImplementedError()
        elif not is_series2 and not is_timestamp2:
            matches = matches & (((dt2.end_time >= dt1) & (dt2.start_time <= dt1)) | \
                ((dt2.end_time - dt1).abs()>=min_delta) | ((dt2.start_time - dt1).abs()>=min_delta))
        elif not is_series1 and not is_timestamp1:
            matches = matches & (((dt1.end_time >= dt2) & (dt1.start_time <= dt2)) | \
                ((dt1.end_time - dt2).abs()>=min_delta) | ((dt1.start_time - dt2).abs()>=min_delta))
        elif is_series2 and not is_timestamp2:
            matches = matches & (((dt2.dt.end_time >= dt1) & (dt2.dt.start_time <= dt1)) | \
                ((dt2.dt.end_time - dt1).abs()>=min_delta) | ((dt2.dt.start_time - dt1).abs()>=min_delta))
        elif is_series1 and not is_timestamp1:
            matches = matches & (((dt1.dt.end_time >= dt2) & (dt1.dt.start_time <= dt2)) | \
                ((dt1.dt.end_time - dt2).abs()>=min_delta) | ((dt1.dt.start_time - dt2).abs()>=min_delta))
        else:
            matches = matches & ((dt1 - dt2).abs()>=min_delta)

    return matches

def filter_by_date(df_test, date_col, min_date):
    if isinstance(df_test[date_col].dtype, pd.PeriodDtype):
        df_test = df_test[df_test[date_col].dt.start_time >= min_date]
    else:
        df_test = df_test[df_test[date_col].dt.tz_localize(None) >= min_date]
    return df_test


def find_date_matches(df_test, date_col, date):
    date = date.replace(hour=0, minute=0, second=0)
    if isinstance(df_test[date_col].dtype, pd.PeriodDtype):
        df_matches = df_test[(df_test[date_col].dt.start_time <= date) & (df_test[date_col].dt.end_time >= date)]
    else:
        dates_test = df_test[date_col].dt.tz_localize(None).apply(lambda x: x.replace(hour=0, minute=0, second=0))
        df_matches = df_test[dates_test == date]

    return df_matches


def get_opd_race_col(df_matches):
    return race_col if role_col not in df_matches else opd.defs.columns.RE_GROUP_OFFICER_SUBJECT 

def get_opd_gender_col(df_matches):
    return gender_col if role_col not in df_matches else opd.defs.columns.GENDER_OFFICER_SUBJECT

def get_opd_age_col(df_matches):
    return age_col if role_col not in df_matches else opd.defs.columns.AGE_OFFICER_SUBJECT

def not_equal(a,b):
    # Only 1 is null or values are not equal
    return (pd.isnull(a) != pd.isnull(b)) or \
        (pd.notnull(a) and pd.notnull(b) and a!=b)

def remove_officer_rows(df_test):
    # For columns with subject and officer data in separate rows, remove officer rows
    if role_col in df_test:
        df_test = df_test[df_test[role_col]==opd.defs.get_roles().SUBJECT]
    return df_test


def _compare_values(orig_val, db_val, idx,
                    col, db_col, rcol, gcol, acol, race_only_col,
                    df_matches, num_matches, is_unknown, is_match, is_diff_race,
                    allowed_replacements, check_race_only, inexact_age, max_age_diff,
                    allow_race_diff):
    if col==rcol:
        # Check for comma-separated list
        orig_val = orig_val.split(',')
    else:
        orig_val = [orig_val]

    for val in orig_val:
        if db_col in allowed_replacements and \
            any([val in x and db_val in x for x in allowed_replacements[db_col]]):
            # Allow values in allowed_replacements to be swapped
            return
        elif col==rcol and check_race_only and race_only_col in df_matches and \
            df_matches.loc[idx,race_only_col]==db_val:
            num_matches[idx]+=1
            return
        elif (pd.isnull(db_val) or db_val in ["UNKNOWN",'UNSPECIFIED']) and \
            val not in ["UNKNOWN",'UNSPECIFIED']:
            is_unknown[idx] = True
            return
        elif col==acol and isinstance(db_val, numbers.Number) and isinstance(val, numbers.Number) and \
            pd.notnull(db_val) and pd.notnull(val):
            if inexact_age:
                # Allow year in df_match to be an estimate of the decade so 30 would be a match for any value from 30-39
                is_match[idx] = val == math.floor(db_val/10)*10
            else:
                is_match[idx] = abs(val - db_val)<=max_age_diff
            return
        elif col==rcol and val in race_vals and db_val in race_vals:
            if allow_race_diff:
                is_diff_race[idx] = True
            else:
                is_match[idx] = False
            return
        elif col==rcol and (val.upper() in ["UNKNOWN",'UNSPECIFIED','OTHER','PENDING RELEASE'] or pd.isnull(val)):
            return
        elif col==gcol and val in gender_vals and db_val in gender_vals:
            is_match[idx] = False
            return
        elif col==gcol and (val.upper() in ["UNKNOWN",'UNSPECIFIED','OTHER','PENDING RELEASE'] or pd.isnull(val)):
            return
        elif col==acol and pd.isnull(val) and pd.notnull(db_val):
            return
    else:
        raise NotImplementedError(f"{col} not equal: OPD: {val} vs. {db_val}")


def check_for_match(df_matches, row_match, max_age_diff=0, allowed_replacements=[],
                    check_race_only=False, inexact_age=False, allow_race_diff=False):
    is_unknown = pd.Series(False, index=df_matches.index)
    is_diff_race = pd.Series(False, index=df_matches.index)
    is_match = pd.Series(True, index=df_matches.index)
    num_matches = pd.Series(0, index=df_matches.index)

    race_only_col = opd.defs.columns.RACE_SUBJECT if role_col not in df_matches else opd.defs.columns.RACE_OFFICER_SUBJECT
    rcol = get_opd_race_col(df_matches)
    gcol = get_opd_gender_col(df_matches)
    acol = get_opd_age_col(df_matches)

    for col, db_col in zip([rcol, gcol, acol], [race_col, gender_col, age_col]):
        if col not in df_matches:
            continue
        
        for idx in df_matches.index:
            if not_equal(df_matches.loc[idx, col], row_match[db_col]):
                _compare_values(df_matches.loc[idx, col], row_match[db_col], idx,
                    col, db_col, rcol, gcol, acol, race_only_col,
                    df_matches, num_matches, is_unknown, is_match, is_diff_race,
                    allowed_replacements, check_race_only, inexact_age, max_age_diff,
                    allow_race_diff)
            else:
                num_matches[idx]+=1
                
    return is_match, is_unknown, num_matches, is_diff_race

def columns_for_duplicated_check(t, df_matches_raw):
    # Multiple cases may be due to multiple officers shooting. MPV data appears to be per person killed so need to removed duplicates

    # Start with null values that may only be missing in some records
    ignore_cols = df_matches_raw.columns[df_matches_raw.isnull().any()].tolist()
    keep_cols = []
    for c in t.get_transform_map():
        # Looping over list of columns that were standardized which provides old and new column names
        if c.new_column_name==opd.defs.columns.INJURY_SUBJECT:
            # Same subject can have multiple injuries
            ignore_cols.append(c.new_column_name)
            ignore_cols.append("RAW_"+c.orig_column_name)
        elif "SUBJECT" in c.new_column_name:
            keep_cols.append(c.new_column_name)
        if "OFFICER" in c.new_column_name:
            # Officer columns may differ if multiple officers were involved
            if isinstance(c.orig_column_name,str):
                ignore_cols.append("RAW_"+c.orig_column_name)  # Original column gets renamed
            ignore_cols.append(c.new_column_name)
        elif c.new_column_name=="TIME":
            # Times can be different if person is shot multiple times
            ignore_cols.append(c.new_column_name)
            ignore_cols.append("RAW_"+c.orig_column_name)
        elif c.new_column_name=="DATETIME":
            ignore_cols.append(c.new_column_name)

    for c in df_matches_raw.columns:
        # Remove potential record IDs
        notin = ["officer", "narrative", "objectid", "incnum", 'text', ' hash', 
                 'firearm','longitude','latitude','rank', 'globalid','rin',
                 'description','force','ofc','sworn','emp','weapon','shots','reason']
        if c not in ignore_cols and c not in keep_cols and \
            ("ID" in [x.upper() for x in split_words(c)] or c.lower().startswith("off") or \
             any([x in c.lower() for x in notin]) or c.lower().startswith('raw_')):
            ignore_cols.append(c)

    test_cols = [x for x in df_matches_raw.columns if x not in ignore_cols or x in keep_cols]
    return test_cols, ignore_cols


def match_street_word(x,y):
    if not (match:=x==y) and x[0].isdigit() and y[0].isdigit():
        # Handle cases such as matching 37th and 37
        match = (m:=re.search(r'^(\d+)[a-z]*$',x,re.IGNORECASE)) and \
                (n:=re.search(r'^(\d+)[a-z]*$',y,re.IGNORECASE)) and \
                m.group(1)==n.group(1)
    return match

def street_match(address, col_name, col, notfound='ignore', match_addr_null=False, match_col_null=True):
    addr_tags, addr_type = address_parser.tag(address, col_name)

    matches = pd.Series(False, index=col.index, dtype='object')
    if isinstance(addr_tags, list):
        for t in addr_tags:
            matches |= street_match(" ".join(t.values()), col_name, col, notfound, match_addr_null, match_col_null)
        return matches
    keys_check1 = [x for x in addr_tags.keys() if x.endswith('StreetName')]
    if len(keys_check1)==0:
        if notfound=='error' and addr_type!='Coordinates':
            raise ValueError(f"'StreetName' not found in {address}")
        else:
            return pd.Series(match_addr_null, index=col.index, dtype='object')
    for idx in col.index:
        ctags_all, ctype_all = address_parser.tag(col[idx], col.name)
        if not isinstance(ctags_all, list):
            ctags_all = [ctags_all]
            ctype_all = [ctype_all]
        for ctags, ctype in zip(ctags_all, ctype_all):
            keys_check2 = [x for x in ctags.keys() if x.endswith('StreetName')]
            if ctype in ['Null','Coordinates']:
                if match_col_null:
                    matches[idx] = True
                continue
            if notfound=='error' and len(keys_check2)==0:
                raise ValueError(f"'StreetName' not found in {col[idx]}")

            for k1 in keys_check1:
                words1 = split_words(addr_tags[k1].lower())
                for k2 in keys_check2:
                    words2 = split_words(ctags[k2].lower())
                    for j,w in enumerate(words2[:len(words2)+1-len(words2)]):   # Indexing is to ensure that remaining words can be matched
                        if match_street_word(w, words1[0]):
                            # Check that rest of word matches
                            for l in range(1, len(words1)):
                                if not match_street_word(words1[l], words2[j+l]):
                                    break
                            else:
                                matches[idx] = True
                                break
                    if matches[idx]:
                        break
                if matches[idx]:
                    break
            if matches[idx]:
                break

    return matches