import pytest

from task_flow.ir import GraphIR, NodeIR


def test_graph_rejects_duplicate_ids():
    node = NodeIR("input.a", "input", "a", None, ())
    with pytest.raises(ValueError, match="unique"):
        GraphIR("1.0", "bad", (node, node), (("a", "input.a"),), ("input.a",), "single")


def test_graph_rejects_missing_dependency():
    node = NodeIR("call.f", "call", "f", lambda: None, ("missing",))
    with pytest.raises(ValueError, match="missing"):
        GraphIR("1.0", "bad", (node,), (), ("call.f",), "single")


def test_node_kind_is_validated():
    with pytest.raises(ValueError, match="kind"):
        NodeIR("bad", "unknown", "bad", None, ())
