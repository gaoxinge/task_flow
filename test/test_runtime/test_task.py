import unittest
from operator import add, sub, mul, floordiv
from task_flow import Task, InputTask, ReturnTask, Graph


class TestTask(unittest.TestCase):

    def test_task(self):
        with Graph(name="test") as graph:
            _int1 = InputTask("int1", int)
            _int2 = InputTask("int2", int)
            _add = Task("add", add, _int1, _int2)
            _sub = Task("sub", sub, _int1, _int2)
            _mul = Task("mul", mul, _int1, _int2, execute="process")
            _div = Task("div", floordiv, _int1, _int2, execute="process")
            _print = Task("print", print, _add, _sub, _mul, _div)
            graph.show("result/test.gv")

    def test_return_task(self):
        with Graph(name="test_return") as graph:
            _int1 = InputTask("int1", int)
            _int2 = InputTask("int2", int)
            _add = ReturnTask("add", add, _int1, _int2)
            graph.show("result/test_return.gv")
