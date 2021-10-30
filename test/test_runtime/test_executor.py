import unittest
from operator import add, sub, mul, floordiv
from task_flow import InputTask, ReturnTask, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor, HyperExecutor


class TestExecutor(unittest.TestCase):

    def test_simple_executor(self):
        with SimpleExecutor() as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = ReturnTask("add", add, _int1, _int2)
                _sub = ReturnTask("sub", sub, _int1, _int2)
                _mul = ReturnTask("mul", mul, _int1, _int2, execute="process")
                _div = ReturnTask("div", floordiv, _int1, _int2, execute="process")

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
                _add = ReturnTask("add", add, _int1, _int2)
                _sub = ReturnTask("sub", sub, _int1, _int2)
                _mul = ReturnTask("mul", mul, _int1, _int2, execute="process")
                _div = ReturnTask("div", floordiv, _int1, _int2, execute="process")

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
                _add = ReturnTask("add", add, _int1, _int2)
                _sub = ReturnTask("sub", sub, _int1, _int2)
                _mul = ReturnTask("mul", mul, _int1, _int2, execute="process")
                _div = ReturnTask("div", floordiv, _int1, _int2, execute="process")

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
                _add = ReturnTask("add", add, _int1, _int2)
                _sub = ReturnTask("sub", sub, _int1, _int2)
                _mul = ReturnTask("mul", mul, _int1, _int2, execute="process")
                _div = ReturnTask("div", floordiv, _int1, _int2, execute="process")

                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
                print("hyper executor get result %s" % outputs_map)

                self.assertEqual(outputs_map["add"], 3)
                self.assertEqual(outputs_map["sub"], 1)
                self.assertEqual(outputs_map["mul"], 2)
                self.assertEqual(outputs_map["div"], 2)
