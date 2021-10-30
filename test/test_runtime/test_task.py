import unittest
from operator import add, sub, mul, floordiv
from task_flow import InputTask, NamedInputTask, ReturnTask, Task, Graph


class TestTask(unittest.TestCase):

    def test_task(self):
        with Graph(name="test") as graph:
            _int1 = InputTask(int)
            _int2 = InputTask(int)
            _add = Task(add, _int1, _int2)
            _sub = Task(sub, _int1, _int2)
            _mul = Task(mul, _int1, _int2, execute="process")
            _div = Task(floordiv, _int1, _int2, execute="process")
            _print = Task(print, _add, _sub, _mul, _div)
            graph.show("result/test.gv")

    def test_named_input_task(self):
        with Graph(name="test") as graph:
            _int1 = NamedInputTask("int1", int)
            _int2 = NamedInputTask("int2", int)
            _add = Task(add, _int1, _int2)
            _sub = Task(sub, _int1, _int2)
            _mul = Task(mul, _int1, _int2, execute="process")
            _div = Task(floordiv, _int1, _int2, execute="process")
            _print = Task(print, _add, _sub, _mul, _div)
            graph.show("result/test_named_input.gv")

    def test_return_task(self):
        with Graph(name="test_return") as graph:
            _int1 = InputTask(int)
            _int2 = InputTask(int)
            _add = ReturnTask(add, _int1, _int2)
            graph.show("result/test_return.gv")
