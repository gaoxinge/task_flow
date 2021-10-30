import unittest
import ast
from task_flow import Graph, SimpleExecutor, Transformer, transform


def g(a, b):
    return a - b


code = """
def f(a, b):
    c = 1
    d = a + c
    e = g(b, c)
    return d, e
"""


@transform(globals())
def f(a, b):
    c = 1
    d = a + c
    e = g(b, c)
    return d, e


class TestTransform(unittest.TestCase):

    def test_task_transformer(self):
        with Graph("test") as graph:
            root = ast.parse(code)
            transformer = Transformer(globals())
            transformer.visit(root)
            graph.show("result/task.gv")

    def test_executor_transformer(self):
        with SimpleExecutor() as executor:
            with Graph("test") as graph:
                root = ast.parse(code)
                transformer = Transformer(globals())
                transformer.visit(root)

                x, y = executor.run(graph, inputs_tuple=([2], [1]), inputs_map={})
                self.assertEqual(x, 3)
                self.assertEqual(y, 0)

    def test_transform(self):
        x, y = f(2, 1)
        self.assertEqual(x, 3)
        self.assertEqual(y, 0)
