from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED

__all__ = ["Executor", "SimpleExecutor", "ThreadExecutor", "ProcessExecutor"]


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


class ThreadExecutor(Executor):

    def __init__(self, num):
        self.thread_pool = ThreadPoolExecutor(max_workers=num)

    def run(self, graph):
        _stack = [root for root in graph.roots]
        _futures = [self.thread_pool.submit(root.f) for root in graph.roots]
        _degree = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        _output = {}
        while len(_stack) != 0:
            wait(_futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, _future in enumerate(_futures) if _future.done())
            task = _stack.pop(i)
            _future = _futures.pop(i)
            _output[task.name] = _future.result()

            for child in task.children:
                _degree[child.name] -= 1
                if _degree[child.name] == 0:
                    _inputs = [_output[parent.name] for parent in child.parents]
                    _stack.append(child)
                    _futures.append(self.thread_pool.submit(child.f, *_inputs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_pool.__exit__(exc_type, exc_val, exc_tb)


class ProcessExecutor(Executor):

    def __init__(self, num):
        self.process_pool = ProcessPoolExecutor(max_workers=num)

    def run(self, graph):
        _stack = [root for root in graph.roots]
        _futures = [self.process_pool.submit(root.f) for root in graph.roots]
        _degree = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        _output = {}
        while len(_stack) != 0:
            wait(_futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, _future in enumerate(_futures) if _future.done())
            task = _stack.pop(i)
            _future = _futures.pop(i)
            _output[task.name] = _future.result()

            for child in task.children:
                _degree[child.name] -= 1
                if _degree[child.name] == 0:
                    _inputs = [_output[parent.name] for parent in child.parents]
                    _stack.append(child)
                    _futures.append(self.process_pool.submit(child.f, *_inputs))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process_pool.__exit__(exc_type, exc_val, exc_tb)
