import ast
import inspect
from ..runtime import Task, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor, HyperExecutor
from ..lang import *

__all__ = [
    "Transformer",
    "transform",
]


class Transformer(ast.NodeTransformer):

    def __init__(self, env):
        self.visible = {}
        self.env = env

    def visit_Module(self, node):
        for stmt in node.body:
            self.visit(stmt)

    def visit_FunctionDef(self, node):
        for arg in node.args.args:
            self.visible[arg.arg] = EchoInputTask()
        for stmt in node.body:
            self.visit(stmt)
        return node

    def visit_Name(self, node):
        return self.visible.get(node.id, node.id)

    def visit_Constant(self, node):
        return ConstantTask(node.value)

    def visit_Call(self, node):
        func = self.env[node.func.id]
        args = [self.visible[arg.id] for arg in node.args]
        return Task(func, *args)

    def visit_BinOp(self, node):
        left_task = self.visit(node.left)
        right_task = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return AddTask(left_task, right_task)
        if isinstance(node.op, ast.Sub):
            return SubTask(left_task, right_task)
        if isinstance(node.op, ast.Mult):
            return MulTask(left_task, right_task)
        if isinstance(node.op, ast.Div):
            return TrueDivTask(left_task, right_task)
        if isinstance(node.op, ast.FloorDiv):
            return FloorDivTask(left_task, right_task)
        raise Exception("unknown operation %s" % node.op)

    def visit_Assign(self, node):
        target = node.targets[0]
        if isinstance(target, ast.Tuple) and isinstance(node.value, ast.Tuple):
            for n, t in zip(target.elts, node.value.elts):
                name = self.visit(n)
                task = self.visit(t)
                self.visible[name] = task
            return
        name = self.visit(target)
        task = self.visit(node.value)
        self.visible[name] = task

    def visit_Return(self, node):
        if node.value is None:
            return
        if isinstance(node.value, ast.Tuple):
            for expr in node.value.elts:
                task = self.visit(expr)
                EchoReturnTask(task)
            return
        if isinstance(node.value, ast.List):
            for expr in node.value.elts:
                task = self.visit(expr)
                EchoReturnTask(task)
            return
        self.visit(node.value)


def transform(env={}, execute="simple", executor_args=[], executor=None):
    def dec(f):
        def g(*args, **kwargs):
            if executor is None:
                if execute == "simple":
                    executor_class = SimpleExecutor
                elif execute == "thread":
                    executor_class = ThreadExecutor
                elif execute == "process":
                    executor_class = ProcessExecutor
                elif execute == "hyper":
                    executor_class = HyperExecutor
                else:
                    raise Exception("unknown execute %s" % execute)

                with executor_class(*executor_args) as executor0:
                    with Graph("test") as graph:
                        src = inspect.getsource(f)
                        root = ast.parse(src)
                        transformer = Transformer(env)
                        transformer.visit(root)
                        inputs_tuple = tuple([arg] for arg in args)
                        return executor0.run(graph, inputs_tuple=inputs_tuple, inputs_map={})
            else:
                with Graph("test") as graph:
                    src = inspect.getsource(f)
                    root = ast.parse(src)
                    transformer = Transformer(env)
                    transformer.visit(root)
                    inputs_tuple = tuple([arg] for arg in args)
                    return executor.run(graph, inputs_tuple=inputs_tuple, inputs_map={})
        return g
    return dec
