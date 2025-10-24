import pandas as pd
def attach_node_metrics(nodes_csv: str, dq_csv: str, out_csv: str):
    nodes = pd.read_csv(nodes_csv)
    nodes[['schema','table']] = nodes['node'].str.split('.', n=1, expand=True)
    nodes['table'] = nodes['table'].fillna('')
    # remove layer suffixes for a loose match
    nodes['table'] = nodes['table'].str.replace('_bronze_|_silver_|_gold_|_bi_', '_', regex=True)
    dq = pd.read_csv(dq_csv)
    merged = nodes.merge(dq, on=['schema','table'], how='left')
    merged.to_csv(out_csv, index=False)
