"""Visualize module and lineage graphs with Pyvis. Requires: uv sync --extra viz"""
import json
import sys
from pathlib import Path

from pyvis.network import Network


def load_cartography(cartography_dir: Path):
    cartography_dir = Path(cartography_dir)
    with open(cartography_dir / "module_graph.json", "r", encoding="utf-8") as f:
        module_graph = json.load(f)
    with open(cartography_dir / "lineage_graph.json", "r", encoding="utf-8") as f:
        lineage_graph = json.load(f)
    return module_graph, lineage_graph


def build_module_network(module_graph: dict, out_html: Path):
    net = Network(height="800px", width="100%", bgcolor="#111111", font_color="#FFFFFF")
    net.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=120, spring_strength=0.05)

    # Add nodes
    for node in module_graph.get("nodes", []):
        nid = node["path"]
        label = nid
        title = (node.get("purpose_statement") or "") + "<br>" + f"LOC: {node.get('lines_of_code', 0)}"
        size = max(5, min(40, (node.get("lines_of_code") or 0) / 20))
        net.add_node(nid, label=label, title=title, size=size)

    # Add edges
    for e in module_graph.get("edges", []):
        net.add_edge(e["source"], e["target"], title=e.get("edge_type", ""))

    out_html.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out_html), open_browser=False, notebook=False)


def build_lineage_network(lineage_graph: dict, out_html: Path):
    net = Network(height="800px", width="100%", bgcolor="#111111", font_color="#FFFFFF")
    net.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=140, spring_strength=0.06)

    # Datasets
    for d in lineage_graph.get("datasets", []):
        nid = d["name"]
        cols = d.get("columns", [])
        title = f"Dataset: {nid}<br>Columns: {', '.join(cols[:10])}"
        net.add_node(nid, label=nid, title=title, color="#4CAF50", shape="box")

    # Transformations
    for t in lineage_graph.get("transformations", []):
        nid = t["id"]
        title = f"Transformation: {nid}<br>Type: {t.get('transformation_type')}"
        net.add_node(nid, label="T", title=title, color="#FFC107", shape="dot")

    # Edges
    for e in lineage_graph.get("edges", []):
        src = e["source"]
        tgt = e["target"]
        etype = e.get("edge_type", "")
        color = "#2196F3" if etype == "CONSUMES" else "#E91E63"
        net.add_edge(src, tgt, title=etype, color=color)

    out_html.parent.mkdir(parents=True, exist_ok=True)
    net.write_html(str(out_html), open_browser=False, notebook=False)


if __name__ == "__main__":
    # Default: run for both LLM and no-LLM outputs; or pass one path: python visualize_network.py jaffle_shop_llm/.cartography
    roots = sys.argv[1:] if len(sys.argv) > 1 else ["jaffle_shop_llm", "jaffle_shop_no_llm"]
    for root in roots:
        cartography = Path(root) / ".cartography" if not root.endswith(".cartography") else Path(root)
        if not (cartography / "module_graph.json").exists():
            print(f"Skip (missing): {cartography}")
            continue
        try:
            module_graph, lineage_graph = load_cartography(cartography)
            build_module_network(module_graph, cartography / "module_graph.html")
            build_lineage_network(lineage_graph, cartography / "lineage_graph.html")
            print(f"Wrote: {cartography / 'module_graph.html'}")
            print(f"Wrote: {cartography / 'lineage_graph.html'}")
        except Exception as e:
            print(f"Error for {cartography}: {e}")