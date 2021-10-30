from typing import Any, Callable, Generator
from graphviz import Digraph

__all__ = [
    "Task",
    "InputTask",
    "ReturnTask",
    "Graph",
    "Namespace",
]


class Task:

    def __init__(self, name: str, f: Callable, *tasks: 'Task', execute: str = "thread"):
        self.id = 0
        self.name = name
        self.f = f
        self.execute = execute
        self.parents = []
        self.children = []

        graph = _namespace.top()
        graph.add_task(self, *tasks)

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

    def run(self, *inputs: Any):
        return self.f(*inputs)


class InputTask(Task):

    def __init__(self, name: str, f: Callable, execute: str = "thread"):
        super(InputTask, self).__init__(name, f, execute=execute)


class ReturnTask(Task):

    pass


class Graph:

    def __init__(self, name: str):
        self.frozen = False
        self.name = name
        self.id = 0
        self.inputs = []
        self.returns = []
        self.roots = []
        self.names = {}
        self.visible = {}

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
        if self.frozen:
            raise Exception("frozen graph %s" % self.name)

        if isinstance(task, InputTask):
            for input_task in self.inputs:
                if task.name == input_task.name:
                    raise Exception("duplicate input task name %s" % task.name)

        if isinstance(task, ReturnTask):
            for return_task in self.returns:
                if task.name == return_task.name:
                    raise Exception("duplicate return task name %s" % task.name)

        for parent in tasks:
            if self.visible[parent.name] != parent.id:
                raise Exception("non visible parent task %s" % parent.name)
            if isinstance(parent, ReturnTask):
                raise Exception("dependent return task parent %s" % parent.name)

        self.id += 1

        task.id = self.id

        self.names[self.id] = task
        self.visible[task.name] = self.id

        if isinstance(task, InputTask):
            self.inputs.append(task)

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

    def froze(self):
        self.frozen = True


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
