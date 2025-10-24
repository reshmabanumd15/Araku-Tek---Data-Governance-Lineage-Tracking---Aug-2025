import pandas as pd, networkx as nx

def build_graph(edges_csv: str, nodes_csv: str) -> nx.DiGraph:
    G = nx.DiGraph()
    nodes = pd.read_csv(nodes_csv)
    for n in nodes['node']:
        G.add_node(n)
    edges = pd.read_csv(edges_csv)
    for _, r in edges.iterrows():
        G.add_edge(r['src'], r['dst'], op=r['op'])
    return G

def export_graphml(G: 'nx.DiGraph', path: str):
    nx.write_graphml(G, path)
