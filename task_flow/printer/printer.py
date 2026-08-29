import json
from abc import ABC, abstractmethod
from typing import Any, Dict

from task_flow.ir import GraphIR, NodeIR


def _operation_name(node: NodeIR) -> Any:
    if node.operation is None:
        return None
    module = getattr(node.operation, "__module__", "")
    name = getattr(node.operation, "__qualname__", node.operation.__class__.__name__)
    return "%s.%s" % (module, name) if module else name


class Printer(ABC):
    @abstractmethod
    def print(self, graph: GraphIR) -> Any:
        raise NotImplementedError


class DictPrinter(Printer):
    def print(self, graph: GraphIR) -> Dict[str, Any]:
        nodes = []
        for node in graph.nodes:
            item = {
                "id": node.id,
                "kind": node.kind,
                "name": node.name,
                "dependencies": list(node.dependencies),
            }
            operation = _operation_name(node)
            if operation is not None:
                item["operation"] = operation
            if node.kind == "constant":
                item["value"] = node.value
            nodes.append(item)
        return {
            "ir_version": graph.ir_version,
            "name": graph.name,
            "inputs": [{"name": name, "node": node_id} for name, node_id in graph.inputs],
            "outputs": list(graph.outputs),
            "output_kind": graph.output_kind,
            "nodes": nodes,
        }


class JsonPrinter(Printer):
    def print(self, graph: GraphIR) -> str:
        return json.dumps(DictPrinter().print(graph), indent=2, ensure_ascii=False, default=repr)


def _mermaid_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


class MermaidPrinter(Printer):
    def print(self, graph: GraphIR) -> str:
        lines = ["flowchart TD"]
        for index, node in enumerate(graph.nodes):
            alias = "n%s" % index
            shape = (
                '(["%s"])'
                if node.kind == "input"
                else '{{"%s"}}'
                if node.kind == "constant"
                else '["%s"]'
            )
            lines.append("    %s%s" % (alias, shape % _mermaid_text(node.name)))
        aliases = {node.id: "n%s" % index for index, node in enumerate(graph.nodes)}
        for node in graph.nodes:
            for dependency in node.dependencies:
                lines.append("    %s --> %s" % (aliases[dependency], aliases[node.id]))
        return "\n".join(lines)


def _dot_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class GraphvizPrinter(Printer):
    def print(self, graph: GraphIR) -> str:
        lines = ['digraph "%s" {' % _dot_text(graph.name)]
        aliases = {node.id: "n%s" % index for index, node in enumerate(graph.nodes)}
        shapes = {"input": "oval", "constant": "diamond", "call": "box"}
        for node in graph.nodes:
            lines.append(
                '  %s [label="%s", shape=%s];'
                % (aliases[node.id], _dot_text(node.name), shapes[node.kind])
            )
        for node in graph.nodes:
            for dependency in node.dependencies:
                lines.append("  %s -> %s;" % (aliases[dependency], aliases[node.id]))
        lines.append("}")
        return "\n".join(lines)


_PRINTERS = {
    "dict": DictPrinter,
    "json": JsonPrinter,
    "mermaid": MermaidPrinter,
    "graphviz": GraphvizPrinter,
}


def get_printer(format_name: str) -> Printer:
    try:
        return _PRINTERS[format_name]()
    except KeyError as exc:
        raise ValueError(
            "unknown graph format %r; expected one of: %s" % (format_name, ", ".join(_PRINTERS))
        ) from exc
