import ast
from operator import add, sub, mul, floordiv
from task_flow import Graph, Transformer

code = """
def f(a, b):
    _int1 = int(a)
    _int2 = int(b)
    _add = add(_int1, _int2)
    _sub = sub(_int1, _int2)
    _mul = mul(_int1, _int2)
    _div = floordiv(_int1, _int2)
    print(_add, _sub, _mul, _div)
"""

if __name__ == "__main__":
    with Graph(name="test") as graph:
        env = globals()
        env["int"] = int
        env["print"] = print
        root = ast.parse(code)
        transformer = Transformer(env)
        transformer.visit(root)
        graph.show("result/test.gv")
