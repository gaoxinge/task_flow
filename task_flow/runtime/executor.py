import multiprocessing
from abc import ABC, abstractmethod
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from typing import Any, Dict, Optional, Tuple

from task_flow.ir import GraphIR, NodeIR

__all__ = ["Executor", "InlineExecutor", "ProcessExecutor", "ThreadExecutor"]


class Executor(ABC):
    def __enter__(self) -> "Executor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    def close(self) -> None:
        pass

    def run(
        self, function: Any, args: Tuple[Any, ...] = (), kwargs: Optional[Dict[str, Any]] = None
    ) -> Any:
        if not hasattr(function, "graph_ir") or not hasattr(function, "python_function"):
            raise TypeError("Executor.run expects a CompiledFunction")
        kwargs = {} if kwargs is None else dict(kwargs)
        bound = __import__("inspect").signature(function.python_function).bind(*args, **kwargs)
        graph = function.graph_ir  # type: GraphIR
        values = {}  # type: Dict[str, Any]
        for name, node_id in graph.inputs:
            values[node_id] = bound.arguments[name]
        for node in graph.nodes:
            if node.kind == "constant":
                values[node.id] = node.value

        calls = [node for node in graph.nodes if node.kind == "call"]
        waiting = {
            node.id: sum(dependency not in values for dependency in node.dependencies)
            for node in calls
        }
        children = {}  # type: Dict[str, list]
        for node in calls:
            for dependency in node.dependencies:
                children.setdefault(dependency, []).append(node.id)
        node_map = graph.node_map
        pending = {}  # type: Dict[Future, NodeIR]

        def submit(node: NodeIR) -> None:
            inputs = tuple(values[dependency] for dependency in node.dependencies)
            pending[self._submit(node, inputs)] = node

        for node in calls:
            if waiting[node.id] == 0:
                submit(node)

        completed = 0
        while pending:
            future = self._wait_any(tuple(pending))
            node = pending.pop(future)
            values[node.id] = future.result()
            completed += 1
            for child_id in children.get(node.id, ()):
                waiting[child_id] -= 1
                if waiting[child_id] == 0:
                    submit(node_map[child_id])

        if completed != len(calls):
            blocked = sorted(node.id for node in calls if node.id not in values)
            raise ValueError("GraphIR contains a cycle or unresolved dependencies: %s" % blocked)
        return self._build_result(graph, values)

    @staticmethod
    def _build_result(graph: GraphIR, values: Dict[str, Any]) -> Any:
        results = tuple(values[node_id] for node_id in graph.outputs)
        if graph.output_kind == "none":
            return None
        if graph.output_kind == "single":
            return results[0]
        if graph.output_kind == "list":
            return list(results)
        return results

    @abstractmethod
    def _submit(self, node: NodeIR, inputs: Tuple[Any, ...]) -> Future:
        raise NotImplementedError

    @abstractmethod
    def _wait_any(self, futures: Tuple[Future, ...]) -> Future:
        raise NotImplementedError


class InlineExecutor(Executor):
    def _submit(self, node: NodeIR, inputs: Tuple[Any, ...]) -> Future:
        future = Future()
        try:
            future.set_result(node.operation(*inputs))  # type: ignore[misc]
        except BaseException as exc:
            future.set_exception(exc)
        return future

    def _wait_any(self, futures: Tuple[Future, ...]) -> Future:
        return futures[0]


class _PoolExecutor(Executor):
    pool = None

    def _submit(self, node: NodeIR, inputs: Tuple[Any, ...]) -> Future:
        return self.pool.submit(node.operation, *inputs)

    def _wait_any(self, futures: Tuple[Future, ...]) -> Future:
        done, _ = wait(futures, return_when=FIRST_COMPLETED)
        return next(iter(done))

    def close(self) -> None:
        if self.pool is not None:
            self.pool.shutdown(wait=True)


class ThreadExecutor(_PoolExecutor):
    def __init__(self, thread_num: int):
        if thread_num <= 0:
            raise ValueError("thread_num must be positive")
        self.pool = ThreadPoolExecutor(max_workers=thread_num)


class ProcessExecutor(_PoolExecutor):
    def __init__(self, process_num: int):
        if process_num <= 0:
            raise ValueError("process_num must be positive")
        methods = multiprocessing.get_all_start_methods()
        context = multiprocessing.get_context("fork") if "fork" in methods else None
        self.pool = ProcessPoolExecutor(max_workers=process_num, mp_context=context)
