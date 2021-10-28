import unittest
from operator import add, sub, mul, floordiv
from task_flow import InputTask, Task, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor, HyperExecutor


class TestExecutor(unittest.TestCase):

    def test_simple_executor(self):
        with SimpleExecutor() as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = Task("add", add, _int1, _int2, output=True)
                _sub = Task("sub", sub, _int1, _int2, output=True)
                _mul = Task("mul", mul, _int1, _int2, output=True, execute="process")
                _div = Task("div", floordiv, _int1, _int2, output=True, execute="process")
                _print = Task("print", print, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("simple executor get result %s" % outputs_map)

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_thread_executor(self):
        with ThreadExecutor(thread_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = Task("add", add, _int1, _int2, output=True)
                _sub = Task("sub", sub, _int1, _int2, output=True)
                _mul = Task("mul", mul, _int1, _int2, output=True, execute="process")
                _div = Task("div", floordiv, _int1, _int2, output=True, execute="process")
                _print = Task("print", print, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("thread executor get result %s" % outputs_map)

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_process_executor(self):
        with ProcessExecutor(process_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = Task("add", add, _int1, _int2, output=True)
                _sub = Task("sub", sub, _int1, _int2, output=True)
                _mul = Task("mul", mul, _int1, _int2, output=True, execute="process")
                _div = Task("div", floordiv, _int1, _int2, output=True, execute="process")
                _print = Task("print", print, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("process executor get result %s" % outputs_map)

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_hyper_executor(self):
        with HyperExecutor(thread_num=2, process_num=2) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = Task("add", add, _int1, _int2, output=True)
                _sub = Task("sub", sub, _int1, _int2, output=True)
                _mul = Task("mul", mul, _int1, _int2, output=True, execute="process")
                _div = Task("div", floordiv, _int1, _int2, output=True, execute="process")
                _print = Task("print", print, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("hyper executor get result %s" % outputs_map)

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)
