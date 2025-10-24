import pandas as pd

def diff_columns(v1_csv: str, v2_csv: str) -> pd.DataFrame:
    v1 = pd.read_csv(v1_csv)
    v2 = pd.read_csv(v2_csv)
    k = ['schema','table','column']
    v1['__src'] = 'v1'; v2['__src'] = 'v2'
    merged = v1.merge(v2, on=k, how='outer', suffixes=('_v1','_v2'), indicator=True)
    return merged

def summarize_table_changes(tables_v1: str, tables_v2: str) -> pd.DataFrame:
    t1 = pd.read_csv(tables_v1); t2 = pd.read_csv(tables_v2)
    m = t1.merge(t2, on=['schema','table'], how='outer', suffixes=('_v1','_v2'), indicator=True)
    return m
