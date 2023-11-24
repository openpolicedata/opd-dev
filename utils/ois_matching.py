import math
import numbers
import openpolicedata as opd
import pandas as pd

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

def find_address_col(df_test):
    addr_col = [x for x in df_test.columns if "LOCATION" in x.upper()]
    if len(addr_col):
        return addr_col
    addr_col = [x for x in df_test.columns if x.upper() in ["STREET"]]
    if len(addr_col):
        return addr_col
    addr_col = [x for x in df_test.columns if x.upper() in ["ADDRESS"]]
    return addr_col


def get_opd_race_col(df_matches):
    return race_col if role_col not in df_matches else opd.defs.columns.RE_GROUP_OFFICER_SUBJECT 

def get_opd_gender_col(df_matches):
    return gender_col if role_col not in df_matches else opd.defs.columns.GENDER_OFFICER_SUBJECT

def get_opd_age_col(df_matches):
    return age_col if role_col not in df_matches else opd.defs.columns.AGE_OFFICER_SUBJECT

def not_equal(a,b):
    # Only 1 is null or values are not equal
    return (pd.isnull(a)+pd.isnull(b)==1) or a!=b

def remove_officer_rows(df_test):
    # For columns with subject and officer data in separate rows, remove officer rows
    if role_col in df_test:
        df_test = df_test[df_test[role_col]==opd.defs.get_roles().SUBJECT]
    return df_test

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
            if col==acol:  # Allowing age column to not exist
                continue
            raise NotImplementedError(f"No {col} col")
        
        for idx in df_matches.index:
            if not_equal(df_matches.loc[idx, col], row_match[db_col]):
                if db_col in allowed_replacements and \
                    any([df_matches.loc[idx, col] in x and row_match[db_col] in x for x in allowed_replacements[db_col]]):
                    # Allow values in allowed_replacements to be swapped
                    continue
                elif col==rcol and check_race_only and race_only_col in df_matches and \
                    df_matches.loc[idx,race_only_col]==row_match[db_col]:
                    num_matches[idx]+=1
                    continue
                elif (pd.isnull(row_match[db_col]) or row_match[db_col] in ["UNKNOWN",'UNSPECIFIED']) and \
                    df_matches.loc[idx, col] not in ["UNKNOWN",'UNSPECIFIED']:
                    is_unknown[idx] = True
                elif col==acol and isinstance(row_match[db_col], numbers.Number) and isinstance(df_matches.loc[idx, col], numbers.Number) and \
                    pd.notnull(row_match[db_col]) and pd.notnull(df_matches.loc[idx, col]):
                    if inexact_age:
                        # Allow year in df_match to be an estimate of the decade so 30 would be a match for any value from 30-39
                        is_match[idx] = df_matches.loc[idx, col] == math.floor(row_match[db_col]/10)*10
                    else:
                        is_match[idx] = abs(df_matches.loc[idx, col] - row_match[db_col])<=max_age_diff
                elif col==rcol and df_matches.loc[idx, col] in race_vals and row_match[db_col] in race_vals:
                    if allow_race_diff:
                        is_diff_race[idx] = True
                    else:
                        is_match[idx] = False
                elif col==rcol and df_matches.loc[idx, col] in ["UNKNOWN",'UNSPECIFIED','OTHER'] and row_match[db_col] in race_vals:
                    pass
                elif col==gcol and df_matches.loc[idx, col] in gender_vals and row_match[db_col] in gender_vals:
                    is_match[idx] = False
                elif col==acol and pd.isnull(df_matches.loc[idx, col]) and pd.notnull(row_match[db_col]):
                    pass
                else:
                    raise NotImplementedError(f"{col} not equal")
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
        if "SUBJECT" in c.new_column_name:
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
        notin = ["officer", "narrative", "objectid", "incnum", 'text']
        if c not in ignore_cols and c not in keep_cols and \
            ("ID" in [x.upper() for x in opd.utils.split_words(c)] or c.lower().startswith("off") or \
             any([x in c.lower() for x in notin])):
            ignore_cols.append(c)

    test_cols = [x for x in df_matches_raw.columns if x not in ignore_cols or x in keep_cols]
    return test_cols, ignore_cols
