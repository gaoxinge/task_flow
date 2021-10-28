from operator import add, sub, mul, truediv, floordiv
from ..runtime import InputTask, Task

__all__ = [
    "EchoTask",
    "ConstantTask",
    "AddTask",
    "SubTask",
    "MulTask",
    "TrueDivTask",
    "FloorDivTask",
]


class EchoTask(InputTask):

    def __init__(self, name: str, output: bool = False, execute: str = "thread"):
        super(EchoTask, self).__init__(name, lambda _: _, output=output, execute=execute)


class ConstantTask(Task):

    def __init__(self, name: str, value, output: bool = False, execute: str = "thread"):
        super(ConstantTask, self).__init__(name, lambda _: value, output=output, execute=execute)


class AddTask(Task):

    def __init__(self, name: str, task1, task2, output: bool = False, execute: str = "thread"):
        super(AddTask, self).__init__(name, add, task1, task2, output=output, execute=execute)


class SubTask(Task):

    def __init__(self, name: str, task1, task2, output: bool = False, execute: str = "thread"):
        super(SubTask, self).__init__(name, sub, task1, task2, output=output, execute=execute)


class MulTask(Task):

    def __init__(self, name: str, task1, task2, output: bool = False, execute: str = "thread"):
        super(MulTask, self).__init__(name, mul, task1, task2, output=output, execute=execute)


class TrueDivTask(Task):

    def __init__(self, name: str, task1, task2, output: bool = False, execute: str = "thread"):
        super(TrueDivTask, self).__init__(name, truediv, task1, task2, output=output, execute=execute)


class FloorDivTask(Task):

    def __init__(self, name: str, task1, task2, output: bool = False, execute: str = "thread"):
        super(FloorDivTask, self).__init__(name, floordiv, task1, task2, output=output, execute=execute)
