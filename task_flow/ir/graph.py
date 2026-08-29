from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional, Tuple


@dataclass(frozen=True)
class NodeIR:
    id: str
    kind: str
    name: str
    operation: Optional[Callable[..., Any]]
    dependencies: Tuple[str, ...]
    value: Any = None

    def __post_init__(self) -> None:
        if self.kind not in {"input", "constant", "call"}:
            raise ValueError("unknown node kind: %s" % self.kind)
        if self.kind == "call" and self.operation is None:
            raise ValueError("call node %s requires an operation" % self.id)
        if self.kind != "call" and self.operation is not None:
            raise ValueError("%s node %s cannot define an operation" % (self.kind, self.id))


@dataclass(frozen=True)
class GraphIR:
    ir_version: str
    name: str
    nodes: Tuple[NodeIR, ...]
    inputs: Tuple[Tuple[str, str], ...]
    outputs: Tuple[str, ...]
    output_kind: str

    def __post_init__(self) -> None:
        if self.ir_version != "1.0":
            raise ValueError("unsupported GraphIR version: %s" % self.ir_version)
        if self.output_kind not in {"none", "single", "tuple", "list"}:
            raise ValueError("unknown output kind: %s" % self.output_kind)
        node_map = self.node_map
        if len(node_map) != len(self.nodes):
            raise ValueError("GraphIR node IDs must be unique")
        for node in self.nodes:
            for dependency in node.dependencies:
                if dependency not in node_map:
                    raise ValueError("node %s depends on missing node %s" % (node.id, dependency))
        input_ids = []
        input_names = []
        for name, node_id in self.inputs:
            if name in input_names:
                raise ValueError("duplicate input name: %s" % name)
            if node_id in input_ids:
                raise ValueError("duplicate input node: %s" % node_id)
            if node_id not in node_map or node_map[node_id].kind != "input":
                raise ValueError("invalid input node: %s" % node_id)
            input_names.append(name)
            input_ids.append(node_id)
        for output in self.outputs:
            if output not in node_map:
                raise ValueError("invalid output node: %s" % output)
        if self.output_kind == "single" and len(self.outputs) != 1:
            raise ValueError("single output requires one node")
        if self.output_kind == "none" and self.outputs:
            raise ValueError("none output cannot contain output nodes")

    @property
    def node_map(self) -> Dict[str, NodeIR]:
        return {node.id: node for node in self.nodes}

    def __iter__(self) -> Iterator[NodeIR]:
        return iter(self.nodes)
