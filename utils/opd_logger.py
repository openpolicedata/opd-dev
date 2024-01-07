from datetime import datetime
import glob
from hashlib import sha1
import numpy as np
import pandas as pd
import os
import warnings
from . import ois_matching

def log(df, output_dir, base_name, keys=None, add_date=False, only_diffs=False):
    keys = keys if keys else df.columns
    output_name = os.path.join(output_dir, base_name)
    search = output_name+"_*.csv"
    if add_date:
        output_name+="_"+datetime.now().strftime('%Y%m%d')
    output_name += '.csv'

    if only_diffs:
        old_files = glob.glob(search)
        for f in old_files:
            if len(df)==0:
                break
            df_old = pd.read_csv(f, keep_default_na=False, na_values={'',np.nan})
            def convert_to_int(x):
                if isinstance(x,str):
                    try:
                        if float(x)==int(float(x)):
                            x = int(float(x))
                    except ValueError:
                        pass
                return x
            df_old = df_old.apply(lambda x: x.apply(convert_to_int))

            date_cols_added = []
            keys_use = keys.copy()
            for k in [x for x in keys if "DATE" in x.upper()]:
                if k in df_old and k in df:
                    date_cols_added.append(k+"_TMP")
                    keys_use[[j for j,m in enumerate(keys_use) if m==k][0]] = date_cols_added[-1]
                    # Format dates to ensure data types match
                    try:
                        df[date_cols_added[-1]] = pd.to_datetime(df[k], format='ISO8601', utc=True).dt.strftime('%Y%m%d_%H%M%S')
                    except:
                        df[date_cols_added[-1]] = pd.to_datetime(df[k].dt.to_timestamp(), format='ISO8601', utc=True).dt.strftime('%Y%m%d_%H%M%S')
                    df_old[date_cols_added[-1]] = pd.to_datetime(df_old[k], format='ISO8601', utc=True).dt.strftime('%Y%m%d_%H%M%S')

            with warnings.catch_warnings():
                # Ignore warning about all-NA columns
                warnings.simplefilter(action='ignore', category=FutureWarning)
                df_combo = pd.concat([df_old, df], ignore_index=True)
            df_combo = df_combo.apply(lambda x: x.apply(lambda y: str(y) if isinstance(y,dict) else y))
            df_combo = df_combo.drop_duplicates(subset=[k for k in keys_use if k in df_combo])
            df = df_combo.loc[len(df_old):].drop(columns=date_cols_added)

    if len(df)==0:
        return

    if os.path.exists(output_name):
        df_out = pd.concat([pd.read_csv(output_name, keep_default_na=False, na_values={'',np.nan}), df], ignore_index=True)
    else:
        df_out = df.copy()

    df_out.to_csv(output_name, index=False)

def generate_general_output_data(df_save, addr_col):
    opd_race_col = ois_matching.get_race_col(df_save)
    opd_gender_col = ois_matching.get_gender_col(df_save)
    opd_age_col = ois_matching.get_age_col(df_save)
    cols = ['type', 'known_fatal']
    for c in df_save.columns:
        if c.startswith("MPV"):
            cols.append(c)

    df_global = df_save[cols].copy()
    df_global["OPD Date"] = df_save[ois_matching.date_col]
    df_global["OPD Agency"] = df_save['Agency']

    if opd_race_col  in df_save:
        df_global["OPD Race"] = df_save[opd_race_col]
    if opd_gender_col in df_save:
        df_global["OPD Gender"] = df_save[opd_gender_col]
    if opd_age_col  in df_save:
        df_global["OPD Age"] = df_save[opd_age_col]
    if addr_col:
        df_global["OPD Address"] = df_save[addr_col]
    return df_global

def generate_agency_output_data(df_mpv_agency, df_opd, mpv_addr_col, addr_col, mpv_download_date,
                                log_demo_diffs, subject_demo_correction, 
                                log_age_diffs, match_with_age_diff, agency, known_fatal):
    mpv_race_col = ois_matching.get_race_col(df_mpv_agency)
    mpv_gender_col = ois_matching.get_gender_col(df_mpv_agency)
    mpv_age_col = ois_matching.get_age_col(df_mpv_agency)
    opd_race_col = ois_matching.get_race_col(df_opd)
    opd_gender_col = ois_matching.get_gender_col(df_opd)
    opd_age_col = ois_matching.get_age_col(df_opd)

    df_save = []
    keys = []
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
        df["MPV DATE"] = df_mpv_agency.loc[df.index][ois_matching.date_col]
        df["MPV RACE"] = df_mpv_agency.loc[df.index][mpv_race_col]
        df["MPV GENDER"] = df_mpv_agency.loc[df.index][mpv_gender_col]
        df["MPV AGE"] = df_mpv_agency.loc[df.index][mpv_age_col]
        df["MPV AGENCY"] = df_mpv_agency.loc[df.index][ois_matching.agency_col]
        df["MPV ADDRESS"] = df_mpv_agency.loc[df.index][mpv_addr_col]
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
        df["MPV DATE"] = df_mpv_agency.loc[df.index][ois_matching.date_col]
        df["MPV RACE"] = df_mpv_agency.loc[df.index][mpv_race_col]
        df["MPV GENDER"] = df_mpv_agency.loc[df.index][mpv_gender_col]
        df["MPV AGE"] = df_mpv_agency.loc[df.index][mpv_age_col]
        df["MPV AGENCY"] = df_mpv_agency.loc[df.index][ois_matching.agency_col]
        df["MPV ADDRESS"] = df_mpv_agency.loc[df.index][mpv_addr_col]
        df_save.append(df)

    if len(df_save)>0:
        df_save = pd.concat(df_save, ignore_index=True)
        df_save['MPV Download Date'] = mpv_download_date
        df_save['Agency'] = agency
        df_save['known_fatal'] = known_fatal

        keys = ["MPV ID", 'type', 'known_fatal', 'Agency', ois_matching.date_col]
        if opd_race_col  in df_save:
            keys.append(opd_race_col )
        if opd_gender_col in df_save:
            keys.append(opd_gender_col)
        if opd_age_col  in df_save:
            keys.append(opd_age_col)
        if addr_col:
            keys.append(addr_col)

        new_cols = ['type', 'known_fatal', 'Agency']
        mpv_cols = [x for x in df_save.columns if x.lower().startswith("mpv")]
        new_cols.extend(mpv_cols)
        new_cols.extend([k for k in keys if k not in new_cols and k in df_save])
        new_cols.extend([x for x in df_save.columns if x not in new_cols])
        df_save = df_save[new_cols]

    return df_save, keys
