import pytest

from task_flow import compile
from task_flow.runtime import InlineExecutor, ProcessExecutor, ThreadExecutor


def calculation(a, b):
    c = 1
    return a + c, b - c, a * b


def single(a, b):
    return a + b


def as_list(a, b):
    return [a + b, a - b]


@pytest.mark.parametrize(
    "factory",
    [InlineExecutor, lambda: ThreadExecutor(2), lambda: ProcessExecutor(2)],
)
def test_executors_share_result_semantics(factory):
    function = compile(calculation)
    with factory() as executor:
        assert executor.run(function, args=(2,), kwargs={"b": 3}) == (3, 2, 6)


def test_single_and_list_results_are_not_forced_to_tuple():
    with InlineExecutor() as executor:
        assert executor.run(compile(single), args=(2, 1)) == 3
        assert executor.run(compile(as_list), args=(2, 1)) == [3, 1]


def test_executor_requires_compiled_function():
    with InlineExecutor() as executor, pytest.raises(TypeError, match="CompiledFunction"):
        executor.run(single, args=(2, 1))
