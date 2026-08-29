import ast
import inspect
import operator
import textwrap
from typing import Any, Callable

from task_flow.ir import GraphIR

from .builder import GraphBuilder


class UnsupportedSyntaxError(ValueError):
    pass


_BINARY_OPERATIONS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
}


class Compiler(ast.NodeVisitor):
    def __init__(self, function: Callable[..., Any]):
        self.function = function
        self.builder = GraphBuilder(function.__name__)
        self.visible = {}  # type: Dict[str, str]
        self.environment = dict(function.__globals__)
        closure = inspect.getclosurevars(function)
        self.environment.update(closure.globals)
        self.environment.update(closure.nonlocals)
        self.environment.update(closure.builtins)
        self._returned = False

    def compile(self) -> GraphIR:
        try:
            source = textwrap.dedent(inspect.getsource(self.function))
        except (OSError, TypeError) as exc:
            raise ValueError(
                "cannot read source for function %s" % self.function.__qualname__
            ) from exc
        module = ast.parse(source)
        function_node = next(
            (node for node in module.body if isinstance(node, ast.FunctionDef)), None
        )
        if function_node is None:
            raise UnsupportedSyntaxError("only regular Python functions are supported")
        self.visit(function_node)
        if not self._returned:
            self.builder.set_outputs((), "none")
        return self.builder.build()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if node.args.posonlyargs or node.args.kwonlyargs or node.args.vararg or node.args.kwarg:
            raise UnsupportedSyntaxError(
                "positional-only, keyword-only and variadic parameters are not supported"
            )
        if node.args.defaults or node.args.kw_defaults:
            raise UnsupportedSyntaxError("default parameters are not supported")
        for argument in node.args.args:
            self.visible[argument.arg] = self.builder.add_input(argument.arg)
        for statement in node.body:
            self.visit(statement)
            if self._returned:
                break

    def generic_visit(self, node: ast.AST) -> None:
        raise UnsupportedSyntaxError("unsupported syntax: %s" % type(node).__name__)

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) != 1:
            raise UnsupportedSyntaxError("chained assignment is not supported")
        target = node.targets[0]
        if isinstance(target, (ast.Tuple, ast.List)):
            if not isinstance(node.value, type(target)):
                raise UnsupportedSyntaxError("unpacking requires a matching literal")
            if len(target.elts) != len(node.value.elts):
                raise UnsupportedSyntaxError("unpacking target size mismatch")
            for target_item, value_item in zip(target.elts, node.value.elts):
                self._assign_name(target_item, self._compile_expression(value_item))
            return
        self._assign_name(target, self._compile_expression(node.value))

    def _assign_name(self, target: ast.AST, node_id: str) -> None:
        if not isinstance(target, ast.Name):
            raise UnsupportedSyntaxError("only name assignment is supported")
        self.visible[target.id] = node_id

    def visit_Expr(self, node: ast.Expr) -> None:
        self._compile_expression(node.value)

    def visit_Return(self, node: ast.Return) -> None:
        if self._returned:
            raise UnsupportedSyntaxError("multiple return statements are not supported")
        self._returned = True
        if node.value is None or (
            isinstance(node.value, ast.Constant) and node.value.value is None
        ):
            self.builder.set_outputs((), "none")
        elif isinstance(node.value, ast.Tuple):
            self.builder.set_outputs(
                tuple(self._compile_expression(item) for item in node.value.elts), "tuple"
            )
        elif isinstance(node.value, ast.List):
            self.builder.set_outputs(
                tuple(self._compile_expression(item) for item in node.value.elts), "list"
            )
        else:
            self.builder.set_outputs((self._compile_expression(node.value),), "single")

    def _compile_expression(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            if node.id not in self.visible:
                raise UnsupportedSyntaxError("unknown local name: %s" % node.id)
            return self.visible[node.id]
        if isinstance(node, ast.Constant):
            return self.builder.add_constant(node.value)
        if isinstance(node, ast.BinOp):
            operation = _BINARY_OPERATIONS.get(type(node.op))
            if operation is None:
                raise UnsupportedSyntaxError(
                    "unsupported binary operation: %s" % type(node.op).__name__
                )
            return self.builder.add_call(
                operation,
                (self._compile_expression(node.left), self._compile_expression(node.right)),
            )
        if isinstance(node, ast.Call):
            return self._compile_call(node)
        raise UnsupportedSyntaxError("unsupported expression: %s" % type(node).__name__)

    def _compile_call(self, node: ast.Call) -> str:
        if node.keywords:
            raise UnsupportedSyntaxError("keyword call arguments are not supported")
        function = self._resolve_callable(node.func)
        return self.builder.add_call(
            function, tuple(self._compile_expression(item) for item in node.args)
        )

    def _resolve_callable(self, node: ast.AST) -> Callable[..., Any]:
        value = (
            self.environment.get(node.id)
            if isinstance(node, ast.Name)
            else self._resolve_value(node)
            if isinstance(node, ast.Attribute)
            else None
        )
        if not callable(value):
            raise UnsupportedSyntaxError("cannot resolve callable from source")
        return value

    def _resolve_value(self, node: ast.AST) -> Any:
        if isinstance(node, ast.Name):
            if node.id not in self.environment:
                raise UnsupportedSyntaxError("unknown global name: %s" % node.id)
            return self.environment[node.id]
        if isinstance(node, ast.Attribute):
            return getattr(self._resolve_value(node.value), node.attr)
        raise UnsupportedSyntaxError("unsupported callable reference")
