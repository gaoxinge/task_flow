import unittest
from operator import add, sub, mul, floordiv
from task_flow import InputTask, ReturnTask, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor, HyperExecutor


class TestExecutor(unittest.TestCase):

    def test_simple_executor(self):
        with SimpleExecutor() as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = ReturnTask(add, _int1, _int2)
                _sub = ReturnTask(sub, _int1, _int2)
                _mul = ReturnTask(mul, _int1, _int2, execute="process")
                _div = ReturnTask(floordiv, _int1, _int2, execute="process")

                x, y, z, w = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
                self.assertEqual(x, 3)
                self.assertEqual(y, 1)
                self.assertEqual(z, 2)
                self.assertEqual(w, 2)

    def test_thread_executor(self):
        with ThreadExecutor(thread_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = ReturnTask(add, _int1, _int2)
                _sub = ReturnTask(sub, _int1, _int2)
                _mul = ReturnTask(mul, _int1, _int2, execute="process")
                _div = ReturnTask(floordiv, _int1, _int2, execute="process")

                x, y, z, w = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
                self.assertEqual(x, 3)
                self.assertEqual(y, 1)
                self.assertEqual(z, 2)
                self.assertEqual(w, 2)

    def test_process_executor(self):
        with ProcessExecutor(process_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = ReturnTask(add, _int1, _int2)
                _sub = ReturnTask(sub, _int1, _int2)
                _mul = ReturnTask(mul, _int1, _int2, execute="process")
                _div = ReturnTask(floordiv, _int1, _int2, execute="process")

                x, y, z, w = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
                self.assertEqual(x, 3)
                self.assertEqual(y, 1)
                self.assertEqual(z, 2)
                self.assertEqual(w, 2)

    def test_hyper_executor(self):
        with HyperExecutor(thread_num=2, process_num=2) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int)
                _int2 = InputTask("int2", int)
                _add = ReturnTask(add, _int1, _int2)
                _sub = ReturnTask(sub, _int1, _int2)
                _mul = ReturnTask(mul, _int1, _int2, execute="process")
                _div = ReturnTask(floordiv, _int1, _int2, execute="process")

                x, y, z, w = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
                self.assertEqual(x, 3)
                self.assertEqual(y, 1)
                self.assertEqual(z, 2)
                self.assertEqual(w, 2)
