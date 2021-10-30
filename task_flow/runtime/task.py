from typing import Any, Callable, Generator
from graphviz import Digraph

__all__ = [
    "InputTask",
    "NamedInputTask",
    "ReturnTask",
    "Task",
    "Graph",
    "Namespace",
]


class Task:

    def __init__(self, f: Callable, *tasks: 'Task', execute: str = "thread"):
        self.id = 0
        self.f = f
        self.execute = execute
        self.parents = []
        self.children = []

        graph = _namespace.top()
        graph.add_task(self, *tasks)

    def __str__(self) -> str:
        s = "%s(\\n" \
            "id=%s,\\n" \
            "executor=%s)"
        t = (
            self.__class__.__name__,
            self.id,
            self.execute
        )
        return s % t

    __repr__ = __str__

    def run(self, *inputs: Any):
        return self.f(*inputs)


class InputTask(Task):

    pass


class NamedInputTask(Task):

    def __init__(self, name: str, f: Callable, execute: str = "thread"):
        self.name = name
        super(NamedInputTask, self).__init__(f, execute=execute)

    def __str__(self) -> str:
        s = "%s(\\n" \
            "id=%s,\\n" \
            "name=%s,\\n" \
            "executor=%s)"
        t = (
            self.__class__.__name__,
            self.id,
            self.name,
            self.execute
        )
        return s % t

    __repr__ = __str__


class ReturnTask(Task):

    pass


class Graph:

    def __init__(self, name: str):
        self.name = name
        self.id = 0
        self.args_inputs = []
        self.kwargs_inputs = []
        self.returns = []
        self.roots = []
        self.names = {}

    def __iter__(self) -> Generator[Task, None, None]:
        for task in self.names.values():
            yield task

    def __enter__(self) -> 'Graph':
        _namespace.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        _namespace.pop()
        return exc_type is None

    def add_task(self, task: Task, *tasks: Task):
        if isinstance(task, NamedInputTask):
            for input_task in self.kwargs_inputs:
                if task.name == input_task.name:
                    raise Exception("duplicated name input task %s" % task)

        for parent in tasks:
            if isinstance(parent, ReturnTask):
                raise Exception("dependent return task parent %s" % parent)

        self.id += 1
        task.id = self.id
        self.names[task.id] = task

        if isinstance(task, InputTask):
            self.args_inputs.append(task)

        if isinstance(task, NamedInputTask):
            self.kwargs_inputs.append(task)

        if isinstance(task, ReturnTask):
            self.returns.append(task)

        if len(tasks) == 0:
            self.roots.append(task)
        else:
            for parent in tasks:
                parent.children.append(task)
                task.parents.append(parent)

    def show(self, filename: str):
        dot = Digraph(self.name)
        for task in self:
            for child in task.children:
                dot.edge(str(task), str(child))
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
