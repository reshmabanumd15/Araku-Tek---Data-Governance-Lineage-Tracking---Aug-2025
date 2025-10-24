import argparse, json
from pathlib import Path
from src.extract.inventory_reader import read_inventory
from src.lineage.build_graph import build_graph, export_graphml
from src.dq.attach_metrics import attach_node_metrics
from src.governance.report import generate_report
from src.governance.change_tracker import diff_columns, summarize_table_changes
from src.audit.logger import log_action

def cmd_inventory(args):
    df = read_inventory(args.dir)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / 'inventory_flat.csv', index=False)
    log_action('artifacts/audit_log.csv', 'cli', 'inventory_read', args.dir, f'rows={len(df)}')
    print(f'Inventory rows: {len(df)} -> {out}/inventory_flat.csv')

def cmd_lineage(args):
    G = build_graph(args.edges, args.nodes)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    export_graphml(G, str(out / 'lineage.graphml'))
    log_action('artifacts/audit_log.csv', 'cli', 'lineage_export', 'graphml', 'ok')
    print(f'GraphML written to {out}/lineage.graphml with {G.number_of_nodes()} nodes / {G.number_of_edges()} edges')

def cmd_attach(args):
    attach_node_metrics(args.nodes, args.dq, args.out)
    log_action('artifacts/audit_log.csv', 'cli', 'attach_dq', args.nodes, 'ok')
    print(f'Node metrics attached -> {args.out}')

def cmd_report(args):
    generate_report(args.catalog, args.dq, args.edges, args.out)
    log_action('artifacts/audit_log.csv', 'cli', 'generate_report', args.out, 'ok')
    print(f'Report generated into {args.out}')

def cmd_diff(args):
    diff = diff_columns(args.v1, args.v2)
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    diff.to_csv(out / 'column_diff.csv', index=False)
    tab = summarize_table_changes(args.t1, args.t2)
    tab.to_csv(out / 'table_diff.csv', index=False)
    log_action('artifacts/audit_log.csv', 'cli', 'schema_diff', args.v2, 'ok')
    print(f'Diff outputs -> {out}')

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)

    inv = sub.add_parser('inventory'); inv.add_argument('--dir', default='data/s3_inventory'); inv.add_argument('--out', default='outputs/inventory'); inv.set_defaults(func=cmd_inventory)
    lin = sub.add_parser('lineage'); lin.add_argument('--edges', default='data/lineage/edges.csv'); lin.add_argument('--nodes', default='data/lineage/nodes.csv'); lin.add_argument('--out', default='artifacts/graph'); lin.set_defaults(func=cmd_lineage)
    att = sub.add_parser('attach'); att.add_argument('--nodes', default='data/lineage/nodes.csv'); att.add_argument('--dq', default='data/dq/table_quality.csv'); att.add_argument('--out', default='artifacts/nodes_with_metrics.csv'); att.set_defaults(func=cmd_attach)
    rep = sub.add_parser('report'); rep.add_argument('--catalog', default='data/catalog'); rep.add_argument('--dq', default='data/dq/table_quality.csv'); rep.add_argument('--edges', default='data/lineage/edges.csv'); rep.add_argument('--out', default='reports'); rep.set_defaults(func=cmd_report)
    dif = sub.add_parser('diff'); dif.add_argument('--v1', default='data/catalog/columns_v1.csv'); dif.add_argument('--v2', default='data/catalog/columns_v2.csv'); dif.add_argument('--t1', default='data/catalog/tables_v1.csv'); dif.add_argument('--t2', default='data/catalog/tables_v2.csv'); dif.add_argument('--out', default='outputs/diffs'); dif.set_defaults(func=cmd_diff)

    args = ap.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
