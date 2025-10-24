import json, pandas as pd
from pathlib import Path

def generate_report(catalog_dir: str, dq_csv: str, lineage_edges: str, out_dir: str):
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    tables = pd.read_csv(Path(catalog_dir) / 'tables_v2.csv')
    cols = pd.read_csv(Path(catalog_dir) / 'columns_v2.csv')
    dq = pd.read_csv(dq_csv)
    edges = pd.read_csv(lineage_edges)

    # KPIs
    kpis = {
        'tables': int(len(tables)),
        'columns': int(len(cols)),
        'tables_with_dq': int(dq['table'].nunique() if not dq.empty else 0),
        'lineage_edges': int(len(edges)),
        'domains': int(tables['domain'].nunique())
    }
    (out / 'kpis.json').write_text(json.dumps(kpis, indent=2))

    # DQ coverage by domain
    cov = tables[['schema','table','domain']].merge(dq[['schema','table']], on=['schema','table'], how='left', indicator=True)
    cov['has_dq'] = (cov['_merge']=='both').astype(int)
    coverage = cov.groupby('domain')['has_dq'].mean().reset_index(name='dq_coverage')
    coverage.to_csv(out / 'dq_coverage_by_domain.csv', index=False)

    # Column types distribution
    cols['type_norm'] = cols['type'].str.replace(r'\(.*\)', '', regex=True)
    dist = cols.groupby('type_norm').size().reset_index(name='count').sort_values('count', ascending=False)
    dist.to_csv(out / 'column_type_distribution.csv', index=False)

    # Joined table quality summary
    dq_sum = dq.groupby('schema').agg({'completeness':'mean','uniqueness':'mean','validity':'mean'}).reset_index()
    dq_sum.to_csv(out / 'dq_summary_by_schema.csv', index=False)
