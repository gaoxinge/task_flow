from operator import add, sub, mul, truediv, floordiv
from ..runtime import InputTask, NamedInputTask, ReturnTask, Task

__all__ = [
    "EchoInputTask",
    "EchoNamedInputTask",
    "EchoReturnTask",
    "ConstantTask",
    "AddTask",
    "SubTask",
    "MulTask",
    "TrueDivTask",
    "FloorDivTask",
]


class EchoInputTask(InputTask):

    def __init__(self, execute: str = "thread"):
        super(EchoInputTask, self).__init__(lambda _: _, execute=execute)


class EchoNamedInputTask(NamedInputTask):

    def __init__(self, name, execute: str = "thread"):
        super(EchoNamedInputTask, self).__init__(name, lambda _: _, execute=execute)


class EchoReturnTask(ReturnTask):

    def __init__(self, task, execute: str = "thread"):
        super(EchoReturnTask, self).__init__(lambda _: _, task, execute=execute)


class ConstantTask(Task):

    def __init__(self, value, execute: str = "thread"):
        super(ConstantTask, self).__init__(lambda: value, execute=execute)


class AddTask(Task):

    def __init__(self, task1, task2, execute: str = "thread"):
        super(AddTask, self).__init__(add, task1, task2, execute=execute)


class SubTask(Task):

    def __init__(self, task1, task2, execute: str = "thread"):
        super(SubTask, self).__init__(sub, task1, task2, execute=execute)


class MulTask(Task):

    def __init__(self, task1, task2, execute: str = "thread"):
        super(MulTask, self).__init__(mul, task1, task2, execute=execute)


class TrueDivTask(Task):

    def __init__(self, task1, task2, execute: str = "thread"):
        super(TrueDivTask, self).__init__(truediv, task1, task2, execute=execute)


class FloorDivTask(Task):

    def __init__(self, task1, task2, execute: str = "thread"):
        super(FloorDivTask, self).__init__(floordiv, task1, task2, execute=execute)
