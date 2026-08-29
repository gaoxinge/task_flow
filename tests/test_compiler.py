import pytest

from task_flow import compile
from task_flow.compiler import UnsupportedSyntaxError


def helper(a, b):
    return a - b


def source_graph(a, b):
    c = 1
    d = a + c
    e = helper(b, c)
    return d, e


def list_graph(a, b):
    return [a + b, a - b]


def none_graph(a):
    helper(a, 1)


def duplicate_graph(a, b):
    c = a + b
    d = a + b
    return c, d


def unsupported_graph(a):
    if a:
        return a


def test_compiler_builds_stable_graph_ir():
    first = compile(source_graph).graph_ir
    second = compile(source_graph).graph_ir
    assert first.ir_version == "1.0"
    assert first.inputs == (("a", "input.a"), ("b", "input.b"))
    assert first.output_kind == "tuple"
    assert [node.id for node in first.nodes] == [node.id for node in second.nodes]
    assert len({node.id for node in first.nodes}) == len(first.nodes)


def test_return_shapes_are_preserved():
    assert compile(list_graph).graph_ir.output_kind == "list"
    assert compile(none_graph).graph_ir.output_kind == "none"


def test_duplicate_operations_receive_unique_ids():
    ids = [node.id for node in compile(duplicate_graph).graph_ir.nodes if node.kind == "call"]
    assert len(ids) == len(set(ids)) == 2


def test_unsupported_syntax_is_explicit():
    with pytest.raises(UnsupportedSyntaxError, match="unsupported"):
        compile(unsupported_graph)
