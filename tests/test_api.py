import json

import pytest

from task_flow import CompiledFunction, InlineExecutor, compile, transform


@compile
def compiled_add(a, b):
    return a + b


@compile(format="mermaid")
def mermaid_add(a, b):
    return a + b


@transform
def inline_add(a, b):
    return a + b


@transform(executor="thread", workers=2)
def thread_add(a, b):
    return a + b


def test_compile_decorator_and_default_printing():
    assert isinstance(compiled_add, CompiledFunction)
    assert json.loads(str(compiled_add))["name"] == "compiled_add"
    assert str(mermaid_add).startswith("flowchart TD")


def test_compiled_function_can_run_explicitly():
    with InlineExecutor() as executor:
        assert executor.run(compiled_add, args=(2,), kwargs={"b": 1}) == 3


def test_transform_decorators_run_directly_and_expose_graph():
    assert inline_add(2, b=1) == 3
    assert thread_add(2, 1) == 3
    assert inline_add.graph_ir is inline_add.compiled.graph_ir


def test_empty_parentheses_are_rejected():
    with pytest.raises(TypeError, match="requires format"):
        compile()
    with pytest.raises(TypeError, match="requires executor"):
        transform()


@pytest.mark.parametrize("format_name", ["yaml", ""])
def test_invalid_format_is_rejected(format_name):
    with pytest.raises(ValueError, match="dict, json, mermaid, graphviz"):
        compile(format=format_name)


def test_invalid_executor_configuration_is_rejected():
    with pytest.raises(ValueError, match="positive integer"):
        transform(executor="thread")
    with pytest.raises(ValueError, match="does not accept workers"):
        transform(executor="inline", workers=1)
