from datetime import datetime
import glob
import numpy as np
import pandas as pd
import os

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
            df_old = pd.read_csv(f, keep_default_na=False, na_values={'',np.nan})

            for k in [x for x in keys if "DATE" in x.upper()]:
                if k in df_old and k in df:
                    if df[k].apply(lambda x: isinstance(x,pd.Period)).any():
                        if df[k][0].freq == 'Y':
                            df_old[k] = pd.to_datetime(df_old[k], format='%Y')
                        else:
                            raise NotImplementedError()
                        df_old[k] = df_old[k].apply(lambda x: pd.Period(x,df[k][0].freq))
                    else:
                        df_old[k] = pd.to_datetime(df_old[k], format='ISO8601')

            df_combo = pd.concat([df_old, df], ignore_index=True)
            df_combo = df_combo.drop_duplicates(subset=[k for k in keys if k in df_combo])
            df = df_combo.loc[len(df_old):]

    if len(df)==0:
        return

    if os.path.exists(output_name):
        df_out = pd.concat([pd.read_csv(output_name, keep_default_na=False, na_values={'',np.nan}), df], ignore_index=True)
    else:
        df_out = df.copy()

    df_out.to_csv(output_name, index=False)