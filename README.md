# Project: Data Governance & Lineage Tracking 

A production-style toolkit for **metadata governance + end-to-end lineage tracking**
on a medallion-style data lake (S3 + Glue Data Catalog) with integration points for
data quality metrics and a governance report. Includes large synthetic inventories,
catalog snapshots, lineage graphs, schema change diffs, Airflow orchestration, and CI.

## Highlights
- **Large datasets**: 30 days of S3 inventory (~**30k objects**) across bronze/silver/gold
- **Glue-like Catalog**: synthetic table/column metadata snapshots (v1 & v2) for change tracking
- **Lineage Graph**: edges across bronze→silver→gold→bi layers, exported as `edges.csv`, `nodes.csv`, and GraphML
- **DQ Integration**: per-table metrics (completeness, uniqueness, validity) attached to nodes
- **Governance Report**: automated rollup to JSON/CSV/Markdown
- **Audit Logging**: append-only logs for governance actions
- **Airflow DAG**: scheduled extraction → lineage build → report → publish artifacts
- **Docs**: framework design, runbooks, monitoring, UX notes for the dashboard
- **CI**: flake8 + pytest for core modules


