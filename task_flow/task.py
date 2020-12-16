from graphviz import Digraph

__all__ = ["Graph", "Task"]


class Global:

    def __init__(self):
        self.stack = []

    def push(self, graph):
        self.stack.append(graph)

    def pop(self):
        return self.stack.pop()

    def top(self):
        return self.stack[-1]


_global = Global()


class Graph:

    def __init__(self, name):
        self.name = name
        self.roots = []
        self.names = {}

    def add(self, task):
        self.roots.append(task)

    def check_name(self, name, task):
        if name in self.names:
            raise Exception("duplicated name %s" % name)
        self.names[name] = task

    def output(self, loop):
        degree = {task.name: len(task.parents) for task in self}
        for task in self.roots:
            loop.put(task)

        while True:
            task = loop.get()
            for child in task.children:
                degree[child.name] -= 1
                if degree[child.name] == 0:
                    loop.put(child)

    def show(self, filename):
        dot = Digraph(self.name)
        for task in self:
            for child in task.children:
                dot.edge(task.name, child.name)
        dot.render(filename)

    def __iter__(self):
        for task in self.names.values():
            yield task

    def __enter__(self):
        _global.push(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _global.pop()
        return self


class Task:

    def __init__(self, name, f, *tasks):
        graph = _global.top()
        graph.check_name(name, self)
        self.name = name
        self.f = f
        self.parents = []
        self.children = []

        if len(tasks) == 0:
            graph.add(self)
        else:
            for task in tasks:
                task.add_child(self)
                self.add_parent(task)

    def add_parent(self, task):
        self.parents.append(task)

    def add_child(self, task):
        self.children.append(task)
