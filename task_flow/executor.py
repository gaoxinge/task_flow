from abc import ABC, abstractmethod

__all__ = ["Executor", "SimpleExecutor"]


class Executor(ABC):

    @abstractmethod
    def run(self, graph):
        raise NotImplemented

    @abstractmethod
    def __enter__(self):
        raise NotImplemented

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplemented


class SimpleExecutor(Executor):

    def run(self, graph):
        _stack = [root for root in graph.roots]
        _degree = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        _output = {}
        while len(_stack) != 0:
            task = _stack.pop(0)
            _inputs = [_output[parent.name] for parent in task.parents]
            _output[task.name] = task.f(*_inputs)
            for child in task.children:
                _degree[child.name] -= 1
                if _degree[child.name] == 0:
                    _stack.append(child)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
