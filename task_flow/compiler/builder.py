import re
from collections import defaultdict
from typing import Any, Callable, Tuple

from task_flow.ir import GraphIR, NodeIR


class GraphBuilder:
    def __init__(self, name: str):
        self.name = name
        self._nodes = []  # type: List[NodeIR]
        self._inputs = []  # type: List[Tuple[str, str]]
        self._outputs = ()  # type: Tuple[str, ...]
        self._output_kind = "none"
        self._counts = defaultdict(int)  # type: DefaultDict[str, int]

    @staticmethod
    def _slug(value: str) -> str:
        value = re.sub(r"[^0-9A-Za-z_.]+", "_", value).strip("_")
        return value or "anonymous"

    def _next_id(self, kind: str, name: str) -> str:
        base = "%s.%s" % (kind, self._slug(name))
        self._counts[base] += 1
        occurrence = self._counts[base]
        return base if occurrence == 1 else "%s.%s" % (base, occurrence)

    def add_input(self, name: str) -> str:
        node_id = self._next_id("input", name)
        self._nodes.append(NodeIR(node_id, "input", name, None, ()))
        self._inputs.append((name, node_id))
        return node_id

    def add_constant(self, value: Any) -> str:
        node_id = self._next_id("constant", type(value).__name__)
        self._nodes.append(NodeIR(node_id, "constant", repr(value), None, (), value=value))
        return node_id

    def add_call(
        self, operation: Callable[..., Any], dependencies: Tuple[str, ...], name: str = ""
    ) -> str:
        operation_name = name or getattr(operation, "__qualname__", operation.__class__.__name__)
        module = getattr(operation, "__module__", "")
        display_name = "%s.%s" % (module, operation_name) if module else operation_name
        node_id = self._next_id("call", display_name)
        self._nodes.append(NodeIR(node_id, "call", display_name, operation, dependencies))
        return node_id

    def set_outputs(self, outputs: Tuple[str, ...], output_kind: str) -> None:
        self._outputs = outputs
        self._output_kind = output_kind

    def build(self) -> GraphIR:
        return GraphIR(
            "1.0",
            self.name,
            tuple(self._nodes),
            tuple(self._inputs),
            self._outputs,
            self._output_kind,
        )
