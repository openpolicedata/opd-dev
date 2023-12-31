from itertools import product
import math
import numbers
import openpolicedata as opd
import pandas as pd
import re
from . import address_parser

# Columns will be standardized below to use these names
date_col = opd.defs.columns.DATE
agency_col = opd.defs.columns.AGENCY
fatal_col = opd.defs.columns.FATAL_SUBJECT
role_col = opd.defs.columns.SUBJECT_OR_OFFICER
injury_cols = [opd.defs.columns.INJURY_SUBJECT, opd.defs.columns.INJURY_OFFICER_SUBJECT]
zip_col = opd.defs.columns.ZIP_CODE

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


def zipcode_isequal(df1, df2, loc1=None, loc2=None, count=None, iloc1=None, iloc2=None):
    assert(count in [None,'all','any','none'] or isinstance(count, numbers.Number))
    
    if zip_col in df1 and zip_col in df2:
        if loc1:
            val1 = df1.loc[loc1, zip_col]
        elif isinstance(iloc1, numbers.Number):
            val1 = df1[zip_col].iloc[iloc1]
        else:
            val1 = df1[zip_col]
        if loc2:
            val2 = df2.loc[loc2, zip_col]
        elif isinstance(iloc2, numbers.Number):
            val2 = df2[zip_col].iloc[iloc2]
        else:
            val2 = df2[zip_col]

        matches = val1==val2

        is_series = isinstance(matches, pd.Series)
        if is_series and count=='all':
            return matches.all()
        elif is_series and count=='any':
            return matches.any()
        elif count=='none':
            if is_series:
                return not matches.any()
            else:
                return not matches
        elif isinstance(count, numbers.Number):
            return matches.sum()==count

        return matches
    return False

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


def get_race_col(df):
    if opd.defs.columns.RE_GROUP_SUBJECT in df:
        return opd.defs.columns.RE_GROUP_SUBJECT
    elif opd.defs.columns.RE_GROUP_OFFICER_SUBJECT in df:
        return opd.defs.columns.RE_GROUP_OFFICER_SUBJECT 
    else:
        return None

def get_gender_col(df):
    if opd.defs.columns.GENDER_SUBJECT in df:
        return opd.defs.columns.GENDER_SUBJECT
    elif opd.defs.columns.GENDER_OFFICER_SUBJECT in df:
        return opd.defs.columns.GENDER_OFFICER_SUBJECT 
    else:
        return None

def get_age_col(df):
    if opd.defs.columns.AGE_RANGE_SUBJECT in df:
        return opd.defs.columns.AGE_RANGE_SUBJECT
    elif opd.defs.columns.AGE_RANGE_OFFICER_SUBJECT in df:
        return opd.defs.columns.AGE_RANGE_OFFICER_SUBJECT
    elif opd.defs.columns.AGE_SUBJECT in df:
        return opd.defs.columns.AGE_SUBJECT
    elif opd.defs.columns.AGE_OFFICER_SUBJECT in df:
        return opd.defs.columns.AGE_OFFICER_SUBJECT
    else:
        return None

def remove_officer_rows(df_test):
    # For columns with subject and officer data in separate rows, remove officer rows
    if role_col in df_test:
        df_test = df_test[df_test[role_col]==opd.defs.get_roles().SUBJECT]
    return df_test


_p_age_range = re.compile(r'^(\d+)\-(\d+)$')
def _compare_values(orig_val1, orig_val2, idx,
                    col1, col2, rcol1, gcol1, acol1, race_only_val1, race_only_val2,
                    is_unknown, is_match, is_diff_race,
                    allowed_replacements, check_race_only, inexact_age, max_age_diff,
                    allow_race_diff, delim1=',', delim2=','):
    # When we reach here, orig_val has already been tested to not equal db_val
    orig_val1 = orig_val1.split(delim1) if isinstance(orig_val1, str) and col1==rcol1 else [orig_val1]
    orig_val2 = orig_val2.split(delim1) if isinstance(orig_val2, str) and col1==rcol1 else [orig_val2]

    is_age_range1 = col1==acol1 and "RANGE" in col1
    is_age_range2 = col1==acol1 and "RANGE" in col2

    unknown_vals = ["UNKNOWN",'UNSPECIFIED','OTHER','PENDING RELEASE']
    other_found = False
    not_equal_found = False
    race_diff_found = False
    for val1, val2 in product(orig_val1, orig_val2):
        val1 = val1.strip() if isinstance(val1, str) else val1
        val2 = val2.strip() if isinstance(val2, str) else val2
        if is_age_range1 and is_age_range2:
            raise NotImplementedError()
        elif is_age_range1 or is_age_range2:
            if pd.isnull(val1) and pd.isnull(val2):
                return
            elif pd.isnull(val1):
                other_found = True
            elif (is_age_range1 and (m:=_p_age_range.search(val1))) or \
                 (is_age_range2 and (m:=_p_age_range.search(val2))):
                if pd.isnull(val2):
                    is_unknown[idx] = True  # db is unknown but val is not
                    return
                else:
                    other_val = val2 if is_age_range1 else val1
                    min_age = int(m.groups()[0])
                    max_age = int(m.groups()[1])
                    if min_age-max_age_diff <= other_val <= max_age+max_age_diff:
                        return  # In range
                    else:
                        not_equal_found = True
            else:
                raise NotImplementedError()
        elif (pd.isnull(val1) and pd.isnull(val2)) or val1==val2:
            return # Values are equal
        elif col2 in allowed_replacements and \
            any([val1 in x and val2 in x for x in allowed_replacements[col2]]):
            # Allow values in allowed_replacements to be swapped
            other_found = True
        elif col1==rcol1 and check_race_only and \
            (
                (race_only_val1 and race_only_val1==val2) or \
                (race_only_val2 and race_only_val2==val1) or \
                (race_only_val1 and race_only_val2 and race_only_val1==race_only_val2)
            ):
            return  # Race-only value matches db
        elif (pd.isnull(val2) or val2 in unknown_vals) and val1 not in unknown_vals:
            is_unknown[idx] = True  # db is unknown but val is not
            return
        elif col1==acol1 and isinstance(val2, numbers.Number) and isinstance(val1, numbers.Number) and \
            pd.notnull(val2) and pd.notnull(val1):
            if inexact_age:
                # Allow year in df_match to be an estimate of the decade so 30 would be a match for any value from 30-39
                is_match_cur = val1 == math.floor(val2/10)*10
            else:
                is_match_cur = abs(val1 - val2)<=max_age_diff
            if is_match_cur:
                is_match[idx] &= is_match_cur
                return
            not_equal_found = True
        elif col1==rcol1 and val1 in race_vals and val2 in race_vals:
            if allow_race_diff:
                race_diff_found = True
            else:
                not_equal_found = True
        elif col1 in [rcol1, gcol1] and (val1.upper() in unknown_vals or pd.isnull(val1)):
            other_found = True
        elif col1==gcol1 and val1 in gender_vals and val2 in gender_vals:
            not_equal_found = True
        elif col1==acol1 and pd.isnull(val1) and pd.notnull(val2):
            other_found = True
        else:
            raise NotImplementedError()
        
    if other_found:
        pass
    elif race_diff_found:
        is_diff_race[idx] = True
    elif not_equal_found:
        is_match[idx] = False
    else:
        raise NotImplementedError(f"{col1} not equal: OPD: {val1} vs. {val2}")


def check_for_match(df, row_match, 
                    max_age_diff=0, allowed_replacements={},
                    check_race_only=False, inexact_age=False, allow_race_diff=False,
                    zip_match=False):
    is_unknown = pd.Series(False, index=df.index)
    is_diff_race = pd.Series(False, index=df.index)
    is_match = pd.Series(True, index=df.index)

    race_only_col_df = opd.defs.columns.RACE_SUBJECT if role_col not in df else opd.defs.columns.RACE_OFFICER_SUBJECT
    rcol_df = get_race_col(df)
    gcol_df = get_gender_col(df)
    acol_df = get_age_col(df)

    rcol_row = get_race_col(row_match)
    gcol_row = get_gender_col(row_match)
    acol_row = get_age_col(row_match)
    race_only_col_row = opd.defs.columns.RACE_SUBJECT if role_col not in row_match else opd.defs.columns.RACE_OFFICER_SUBJECT

    if len(set(allowed_replacements.keys()) - {'race','gender'})>0:
        raise NotImplementedError("Replacements only implemented for race and gener currently")
    if 'race' in allowed_replacements:
        allowed_replacements = allowed_replacements.copy()
        allowed_replacements[rcol_row] = allowed_replacements.pop('race')
    if 'gender' in allowed_replacements:
        allowed_replacements = allowed_replacements.copy()
        allowed_replacements[gcol_row] = allowed_replacements.pop('gender')

    for idx in df.index:
        if zip_match:
            if not (zipcode_isequal(df, row_match, loc1=idx)):
                is_match[idx] = False
        for col_df, col_row in zip([rcol_df, gcol_df, acol_df], [rcol_row, gcol_row, acol_row]):
            if col_df not in df or col_row not in row_match:
                continue
        
            _compare_values(df.loc[idx, col_df], row_match[col_row], idx,
                col_df, col_row, rcol_df, gcol_df, acol_df, 
                df.loc[idx, race_only_col_df] if race_only_col_df in df else None, 
                row_match[race_only_col_row] if race_only_col_row in row_match else None,
                is_unknown, is_match, is_diff_race,
                allowed_replacements, check_race_only, inexact_age, max_age_diff,
                allow_race_diff)
                
    return is_match, is_unknown, is_diff_race

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
                 'description','force','ofc','sworn','emp','weapon','shots','reason',
                 'perceived','armed','nature','level']
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