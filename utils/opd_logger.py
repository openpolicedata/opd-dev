from datetime import datetime
import glob
import numpy as np
import pandas as pd
import os
import warnings

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
                    df[date_cols_added[-1]] = pd.to_datetime(df[k], format='ISO8601', utc=True).dt.strftime('%Y%m%d_%H%M%S')
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