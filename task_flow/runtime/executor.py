from abc import ABC, abstractmethod
from typing import Any, Dict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from .task import InputTask, Graph

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
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError

    @abstractmethod
    def run(self, graph: Graph, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


class SimpleExecutor(Executor):

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def run(self, graph: Graph, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
        ready = [root for root in graph.roots]
        waiting = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        output0 = {}
        output1 = {}
        while len(ready) != 0:
            # step1: get task
            task = ready.pop(0)

            # step2: get inputs
            if isinstance(task, InputTask):
                inputs = inputs_map[task.name]
            else:
                inputs = []
                for parent in task.parents:
                    if not parent.output:
                        inputs.append(output0[parent.name][0])
                        output0[parent.name][1] -= 1
                        if output0[parent.name][1] == 0:
                            del output0[parent.name]
                    else:
                        inputs.append(output1[parent.name])

            # step3: get result
            result = task.f(*inputs)
            if not task.output:
                output0[task.name] = [result, len(task.children)]
            else:
                output1[task.name] = result

            # step4: get ready
            for child in task.children:
                waiting[child.name] -= 1
                if waiting[child.name] == 0:
                    ready.append(child)
                    del waiting[child.name]

        return output1


class ThreadExecutor(Executor):

    def __init__(self, thread_num: int):
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_pool.__exit__(exc_type, exc_val, exc_tb)

    def run(self, graph: Graph, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            futures.append(self.thread_pool.submit(root.f, *inputs))
        waiting = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        output0 = {}
        output1 = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            if not task.output:
                output0[task.name] = [future.result(), len(task.children)]
            else:
                output1[task.name] = future.result()

            for child in task.children:
                waiting[child.name] -= 1
                if waiting[child.name] == 0:
                    inputs = []
                    for parent in child.parents:
                        if not parent.output:
                            inputs.append(output0[parent.name][0])
                            output0[parent.name][1] -= 1
                            if output0[parent.name][1] == 0:
                                del output0[parent.name]
                        else:
                            inputs.append(output1[parent.name])

                    ready.append(child)
                    futures.append(self.thread_pool.submit(child.f, *inputs))
                    del waiting[child.name]

        return output1


class ProcessExecutor(Executor):

    def __init__(self, process_num: int):
        self.process_pool = ProcessPoolExecutor(max_workers=process_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.process_pool.__exit__(exc_type, exc_val, exc_tb)

    def run(self, graph: Graph, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            futures.append(self.process_pool.submit(root.f, *inputs))
        waiting = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        output0 = {}
        output1 = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            if not task.output:
                output0[task.name] = [future.result(), len(task.children)]
            else:
                output1[task.name] = future.result()

            for child in task.children:
                waiting[child.name] -= 1
                if waiting[child.name] == 0:
                    inputs = []
                    for parent in child.parents:
                        if not parent.output:
                            inputs.append(output0[parent.name][0])
                            output0[parent.name][1] -= 1
                            if output0[parent.name][1] == 0:
                                del output0[parent.name]
                        else:
                            inputs.append(output1[parent.name])

                    ready.append(child)
                    futures.append(self.process_pool.submit(child.f, *inputs))
                    del waiting[child.name]

        return output1


class HyperExecutor(Executor):

    def __init__(self, thread_num: int, process_num: int):
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_num)
        self.process_pool = ProcessPoolExecutor(max_workers=process_num)

    def __enter__(self) -> Executor:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_pool.__exit__(exc_type, exc_val, exc_tb)
        self.process_pool.__exit__(exc_type, exc_val, exc_tb)

    def run(self, graph: Graph, inputs_map: Dict[str, Any]) -> Dict[str, Any]:
        ready = [root for root in graph.roots]
        futures = []
        for root in graph.roots:
            if isinstance(root, InputTask):
                inputs = inputs_map[root.name]
            else:
                inputs = []
            if root.execute == "thread":
                futures.append(self.thread_pool.submit(root.f, *inputs))
            else:
                futures.append(self.process_pool.submit(root.f, *inputs))
        waiting = {task.name: len(task.parents) for task in graph if task not in graph.roots}
        output0 = {}
        output1 = {}
        while len(ready) != 0:
            wait(futures, return_when=FIRST_COMPLETED)
            i = next(_ for _, future in enumerate(futures) if future.done())
            task = ready.pop(i)
            future = futures.pop(i)

            if not task.output:
                output0[task.name] = [future.result(), len(task.children)]
            else:
                output1[task.name] = future.result()

            for child in task.children:
                waiting[child.name] -= 1
                if waiting[child.name] == 0:
                    inputs = []
                    for parent in child.parents:
                        if not parent.output:
                            inputs.append(output0[parent.name][0])
                            output0[parent.name][1] -= 1
                            if output0[parent.name][1] == 0:
                                del output0[parent.name]
                        else:
                            inputs.append(output1[parent.name])

                    ready.append(child)
                    if child.execute == "thread":
                        futures.append(self.thread_pool.submit(child.f, *inputs))
                    else:
                        futures.append(self.process_pool.submit(child.f, *inputs))
                    del waiting[child.name]

        return output1
