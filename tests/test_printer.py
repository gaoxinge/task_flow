import json

from task_flow import compile
from task_flow.printer import DictPrinter


def graph(a, b):
    _unused = 1
    return a + b


def test_dict_and_json_include_all_nodes():
    function = compile(graph)
    raw = DictPrinter().print(function.graph_ir)
    assert raw["ir_version"] == "1.0"
    assert len(raw["nodes"]) == 4
    assert json.loads(format(function, "json")) == raw


def test_visual_printers_include_nodes_and_edges():
    function = compile(graph)
    mermaid = format(function, "mermaid")
    dot = format(function, "graphviz")
    assert mermaid.startswith("flowchart TD")
    assert "-->" in mermaid
    assert dot.startswith('digraph "graph"')
    assert "->" in dot


def test_dict_format_is_printable_text():
    function = compile(graph)
    assert format(function, "dict").startswith("{'ir_version': '1.0'")
