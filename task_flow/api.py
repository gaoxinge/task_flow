import functools
from typing import Any, Callable, Optional

from task_flow.compiler import Compiler
from task_flow.ir import GraphIR
from task_flow.printer import get_printer
from task_flow.runtime import InlineExecutor, ProcessExecutor, ThreadExecutor

_MISSING = object()
_FORMATS = {"dict", "json", "mermaid", "graphviz"}
_EXECUTORS = {"inline", "thread", "process"}


class CompiledFunction:
    def __init__(self, python_function: Callable[..., Any], default_format: str = "json"):
        self.python_function = python_function
        self._graph_ir = Compiler(python_function).compile()
        self.default_format = _validate_format(default_format)
        functools.update_wrapper(self, python_function)

    @property
    def graph_ir(self) -> GraphIR:
        return self._graph_ir

    def __str__(self) -> str:
        return self.__format__("")

    def __format__(self, format_spec: str) -> str:
        format_name = self.default_format if format_spec == "" else _validate_format(format_spec)
        rendered = get_printer(format_name).print(self.graph_ir)
        return repr(rendered) if format_name == "dict" else rendered


class TransformedFunction:
    def __init__(self, compiled: CompiledFunction, executor_name: str, workers: Optional[int]):
        self.compiled = compiled
        self.executor_name = executor_name
        self.workers = workers
        functools.update_wrapper(self, compiled.python_function)

    @property
    def graph_ir(self) -> GraphIR:
        return self.compiled.graph_ir

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        executor = _create_executor(self.executor_name, self.workers)
        with executor:
            return executor.run(self.compiled, args=args, kwargs=kwargs)

    def __str__(self) -> str:
        return str(self.compiled)

    def __format__(self, format_spec: str) -> str:
        return self.compiled.__format__(format_spec)


def _validate_format(format_name: str) -> str:
    if format_name not in _FORMATS:
        raise ValueError(
            "unknown graph format %r; expected one of: dict, json, mermaid, graphviz" % format_name
        )
    return format_name


def compile(function: Any = _MISSING, *, format: Any = _MISSING) -> Any:
    if function is _MISSING:
        if format is _MISSING:
            raise TypeError("compile() requires format when called with parentheses")
        format_name = _validate_format(format)

        def decorator(target: Callable[..., Any]) -> CompiledFunction:
            return CompiledFunction(target, default_format=format_name)

        return decorator
    if format is not _MISSING:
        raise TypeError("use compile(format=...) as a decorator")
    if not callable(function):
        raise TypeError("compile expects a Python function")
    return CompiledFunction(function, default_format="json")


def transform(
    function: Any = _MISSING, *, executor: Any = _MISSING, workers: Optional[int] = None
) -> Any:
    if function is _MISSING:
        if executor is _MISSING:
            raise TypeError("transform() requires executor when called with parentheses")
        executor_name = _validate_executor(executor, workers)

        def decorator(target: Callable[..., Any]) -> TransformedFunction:
            return TransformedFunction(CompiledFunction(target), executor_name, workers)

        return decorator
    if executor is not _MISSING:
        raise TypeError("use transform(executor=...) as a decorator")
    if workers is not None:
        raise TypeError("workers requires an explicit executor")
    if not callable(function):
        raise TypeError("transform expects a Python function")
    return TransformedFunction(CompiledFunction(function), "inline", None)


def _validate_executor(executor_name: str, workers: Optional[int]) -> str:
    if executor_name not in _EXECUTORS:
        raise ValueError(
            "unknown executor %r; expected one of: inline, thread, process" % executor_name
        )
    if executor_name == "inline":
        if workers is not None:
            raise ValueError("inline executor does not accept workers")
    elif not isinstance(workers, int) or isinstance(workers, bool) or workers <= 0:
        raise ValueError("thread and process executors require positive integer workers")
    return executor_name


def _create_executor(executor_name: str, workers: Optional[int]):
    if executor_name == "inline":
        return InlineExecutor()
    if executor_name == "thread":
        return ThreadExecutor(thread_num=workers)  # type: ignore[arg-type]
    return ProcessExecutor(process_num=workers)  # type: ignore[arg-type]
