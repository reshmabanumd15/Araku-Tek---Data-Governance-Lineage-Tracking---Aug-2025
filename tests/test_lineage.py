from src.lineage.build_graph import build_graph
def test_graph_builds(tmp_path):
    edges = tmp_path / 'e.csv'; nodes = tmp_path / 'n.csv'
    edges.write_text('src,dst,op\na,b,transform\n'); nodes.write_text('node\na\n b\n')
    G = build_graph(str(edges), str(nodes))
    assert G.number_of_edges() == 1 and G.number_of_nodes() == 2
