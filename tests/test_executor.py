import time
import unittest
from operator import add, sub, mul, floordiv
from task_flow import InputTask, Task, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor, HyperExecutor


def int0(x):
    time.sleep(3)
    return int(x)


def add0(x, y):
    time.sleep(3)
    return add(x, y)


def sub0(x, y):
    time.sleep(3)
    return sub(x, y)


def mul0(x, y):
    time.sleep(3)
    return mul(x, y)


def div0(x, y):
    time.sleep(3)
    return floordiv(x, y)


def print0(*args):
    time.sleep(3)
    return print(*args)


class TestExecutor(unittest.TestCase):

    def test_simple_executor(self):
        start = time.time()
        with SimpleExecutor() as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int0)
                _int2 = InputTask("int2", int0)
                _add = Task("add", add0, _int1, _int2, output=True)
                _sub = Task("sub", sub0, _int1, _int2, output=True)
                _mul = Task("mul", mul0, _int1, _int2, output=True, execute="process")
                _div = Task("div", div0, _int1, _int2, output=True, execute="process")
                _print = Task("print", print0, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("simple executor consume time: %.2fs, get result %s" % (time.time() - start, outputs_map))

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_thread_executor(self):
        start = time.time()
        with ThreadExecutor(thread_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int0)
                _int2 = InputTask("int2", int0)
                _add = Task("add", add0, _int1, _int2, output=True)
                _sub = Task("sub", sub0, _int1, _int2, output=True)
                _mul = Task("mul", mul0, _int1, _int2, output=True, execute="process")
                _div = Task("div", div0, _int1, _int2, output=True, execute="process")
                _print = Task("print", print0, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("thread executor consume time: %.2fs, get result %s" % (time.time() - start, outputs_map))

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_process_executor(self):
        start = time.time()
        with ProcessExecutor(process_num=3) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int0)
                _int2 = InputTask("int2", int0)
                _add = Task("add", add0, _int1, _int2, output=True)
                _sub = Task("sub", sub0, _int1, _int2, output=True)
                _mul = Task("mul", mul0, _int1, _int2, output=True, execute="process")
                _div = Task("div", div0, _int1, _int2, output=True, execute="process")
                _print = Task("print", print0, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("process executor consume time: %.2fs, get result %s" % (time.time() - start, outputs_map))

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)

    def test_hyper_executor(self):
        start = time.time()
        with HyperExecutor(thread_num=2, process_num=2) as executor:
            with Graph(name="test") as graph:
                _int1 = InputTask("int1", int0)
                _int2 = InputTask("int2", int0)
                _add = Task("add", add0, _int1, _int2, output=True)
                _sub = Task("sub", sub0, _int1, _int2, output=True)
                _mul = Task("mul", mul0, _int1, _int2, output=True, execute="process")
                _div = Task("div", div0, _int1, _int2, output=True, execute="process")
                _print = Task("print", print0, _add, _sub, _mul, _div)
                outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print("hyper executor consume time: %.2fs, get result %s" % (time.time() - start, outputs_map))

        self.assertEqual(outputs_map["add"], 3)
        self.assertEqual(outputs_map["sub"], 1)
        self.assertEqual(outputs_map["mul"], 2)
        self.assertEqual(outputs_map["div"], 2)
