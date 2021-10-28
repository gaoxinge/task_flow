from typing import Any, Callable, Generator
from graphviz import Digraph

__all__ = [
    "Task",
    "InputTask",
    "Graph",
    "Namespace",
]


class Task:

    def __init__(self, name: str, f: Callable, *tasks: 'Task', output: bool = False, execute: str = "thread"):
        self.name = name
        self.f = f
        self.output = output
        self.execute = execute
        self.parents = []
        self.children = []

        graph = _namespace.top()
        graph[name] = self
        if len(tasks) == 0:
            graph.add_root(self)
        else:
            for task in tasks:
                task.children.append(self)
                self.parents.append(task)

    def run(self, *inputs: Any):
        return self.f(*inputs)


class InputTask(Task):

    def __init__(self, name: str, f: Callable, output: bool = False, execute: str = "thread"):
        super(InputTask, self).__init__(name, f, output=output, execute=execute)


class Graph:

    def __init__(self, name: str):
        self.name = name
        self.names = {}
        self.roots = []

    def __setitem__(self, name: str, task: Task):
        if name in self.names:
            raise Exception("duplicated name %s" % name)
        self.names[name] = task

    def __getitem__(self, name: str) -> Task:
        return self.names[name]

    def __iter__(self) -> Generator[Task, None, None]:
        for task in self.names.values():
            yield task

    def __enter__(self) -> 'Graph':
        _namespace.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> 'Graph':
        _namespace.pop()
        return self

    def add_root(self, task: Task):
        self.roots.append(task)

    def show(self, filename: str):
        dot = Digraph(self.name)
        s = "name[%s]\\n" \
            "type[%s]\\n" \
            "output[%s]\\n" \
            "executor[%s]"
        for task in self:
            t = (
                task.name,
                task.__class__.__name__,
                task.output,
                task.execute
            )
            task_name = s % t
            for child in task.children:
                t = (
                    child.name,
                    child.__class__.__name__,
                    child.output,
                    child.execute
                )
                child_name = s % t
                dot.edge(task_name, child_name)
        dot.render(filename)


class Namespace:

    def __init__(self):
        self.stack = []

    def push(self, graph: Graph):
        self.stack.append(graph)

    def pop(self) -> Graph:
        return self.stack.pop()

    def top(self) -> Graph:
        return self.stack[-1]


_namespace = Namespace()
