import unittest
from task_flow import (
    Graph, SimpleExecutor, EchoInputTask, EchoReturnTask, ConstantTask,
    AddTask, SubTask, MulTask, TrueDivTask, FloorDivTask
)


class TestCore(unittest.TestCase):

    def test_task_core(self):
        with Graph(name="test_graph") as graph:
            _a = EchoInputTask("a")
            _b = EchoInputTask("b")
            _c = ConstantTask(1)
            _add = AddTask(_a, _b)
            _sub = SubTask(_a, _b)
            _mul = MulTask(_a, _b)
            _true_div = TrueDivTask(_a, _c)
            _floor_div = FloorDivTask(_a, _c)
            _return1 = EchoReturnTask(_add)
            _return2 = EchoReturnTask(_true_div)
            graph.show("result/test_task.gv")

    def test_executor_core(self):
        with SimpleExecutor() as executor:
            with Graph(name="test_graph") as graph:
                _a = EchoInputTask("a")
                _b = EchoInputTask("b")
                _c = ConstantTask(1)
                _add = AddTask(_a, _b)
                _sub = SubTask(_a, _b)
                _mul = MulTask(_a, _b)
                _true_div = TrueDivTask(_a, _c)
                _floor_div = FloorDivTask(_a, _c)
                _return1 = EchoReturnTask(_add)
                _return2 = EchoReturnTask(_true_div)

                x, y = executor.run(graph, inputs_map={"a": [2], "b": [1]})
                self.assertEqual(x, 3)
                self.assertEqual(y, 2)
