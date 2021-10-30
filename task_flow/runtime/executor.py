from abc import ABC, abstractmethod
from typing import List, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from .task import InputTask, NamedInputTask, Graph

__all__ = [
    "Executor",
    "SimpleExecutor",
    "ThreadExecutor",
    "ProcessExecutor",
    "HyperExecutor",
]


class Executor(ABC):

    @abstractmethod
    def __enter__(self) -> 'Executor':
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        raise NotImplementedError

    @abstractmethod
    def run(self, graph: Graph, inputs_tuple: Tuple[List], inputs_map: Dict[str, List]) -> Tuple:
        raise NotImplementedError


class SimpleExecutor(Executor):

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return exc_type is None

    def run(self, graph: Graph, inputs_tuple: Tuple[List], inputs_map: Dict[str, List]) -> Tuple:
        ready = [root for root in graph.roots]
        waiting = {task.id: len(task.parents) for task in graph if task not in graph.roots}
        output = {}
        while len(ready) != 0:
            # step1: get task
            task = ready.pop(0)

            # step2: get inputs
            if isinstance(task, InputTask):
                i = next(i for i, input_task in enumerate(graph.args_inputs) if task.id == input_task.id)
                inputs = inputs_tuple[i]
            elif isinstance(task, NamedInputTask):
                inputs = inputs_map[task.name]
            else:
                inputs = []
                for parent in task.parents:
                    inputs.append(output[parent.id][0])
                    output[parent.id][1] -= 1
                    if output[parent.id][1] == 0:
                        del output[parent.id]

            # step3: get result
            result = task.f(*inputs)
            output[task.id] = [result, len(task.children)]

            # step4: get ready
            for child in task.children:
                waiting[child.id] -= 1
                if waiting[child.id] == 0:
                    ready.append(child)
                    del waiting[child.id]

        return tuple(output[return_task.id][0] for return_task in graph.returns)


class ThreadExecutor(Executor):

    def __init__(self, thread_num: int):
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.thread_pool.__exit__(exc_type, exc_val, exc_tb)
        return exc_type is None

    def run(self, graph: Graph, inputs_tuple: Tuple[List], inputs_map: Dict[str, List]) -> Tuple:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                i = next(i for i, input_task in enumerate(graph.args_inputs) if root.id == input_task.id)
                inputs = inputs_tuple[i]
            elif isinstance(root, NamedInputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            futures.append(self.thread_pool.submit(root.f, *inputs))
        waiting = {task.id: len(task.parents) for task in graph if task not in graph.roots}
        output = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            output[task.id] = [future.result(), len(task.children)]

            for child in task.children:
                waiting[child.id] -= 1
                if waiting[child.id] == 0:
                    inputs = []
                    for parent in child.parents:
                        inputs.append(output[parent.id][0])
                        output[parent.id][1] -= 1
                        if output[parent.id][1] == 0:
                            del output[parent.id]

                    ready.append(child)
                    futures.append(self.thread_pool.submit(child.f, *inputs))
                    del waiting[child.id]

        return tuple(output[return_task.id][0] for return_task in graph.returns)


class ProcessExecutor(Executor):

    def __init__(self, process_num: int):
        self.process_pool = ProcessPoolExecutor(max_workers=process_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.process_pool.__exit__(exc_type, exc_val, exc_tb)
        return exc_type is None

    def run(self, graph: Graph, inputs_tuple: Tuple[List], inputs_map: Dict[str, List]) -> Tuple:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                i = next(i for i, input_task in enumerate(graph.args_inputs) if root.id == input_task.id)
                inputs = inputs_tuple[i]
            elif isinstance(root, NamedInputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            futures.append(self.process_pool.submit(root.f, *inputs))
        waiting = {task.id: len(task.parents) for task in graph if task not in graph.roots}
        output = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            output[task.id] = [future.result(), len(task.children)]

            for child in task.children:
                waiting[child.id] -= 1
                if waiting[child.id] == 0:
                    inputs = []
                    for parent in child.parents:
                        inputs.append(output[parent.id][0])
                        output[parent.id][1] -= 1
                        if output[parent.id][1] == 0:
                            del output[parent.id]

                    ready.append(child)
                    futures.append(self.process_pool.submit(child.f, *inputs))
                    del waiting[child.id]

        return tuple(output[return_task.id][0] for return_task in graph.returns)


class HyperExecutor(Executor):

    def __init__(self, thread_num: int, process_num: int):
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_num)
        self.process_pool = ProcessPoolExecutor(max_workers=process_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.thread_pool.__exit__(exc_type, exc_val, exc_tb)
        self.process_pool.__exit__(exc_type, exc_val, exc_tb)
        return exc_type is None

    def run(self, graph: Graph, inputs_tuple: Tuple[List], inputs_map: Dict[str, List]) -> Tuple:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                i = next(i for i, input_task in enumerate(graph.args_inputs) if root.id == input_task.id)
                inputs = inputs_tuple[i]
            elif isinstance(root, NamedInputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            if root.execute == "thread":
                futures.append(self.thread_pool.submit(root.f, *inputs))
            else:
                futures.append(self.process_pool.submit(root.f, *inputs))
        waiting = {task.id: len(task.parents) for task in graph if task not in graph.roots}
        output = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            output[task.id] = [future.result(), len(task.children)]

            for child in task.children:
                waiting[child.id] -= 1
                if waiting[child.id] == 0:
                    inputs = []
                    for parent in child.parents:
                        inputs.append(output[parent.id][0])
                        output[parent.id][1] -= 1
                        if output[parent.id][1] == 0:
                            del output[parent.id]

                    ready.append(child)
                    if child.execute == "thread":
                        futures.append(self.thread_pool.submit(child.f, *inputs))
                    else:
                        futures.append(self.process_pool.submit(child.f, *inputs))
                    del waiting[child.id]

        return tuple(output[return_task.id][0] for return_task in graph.returns)
