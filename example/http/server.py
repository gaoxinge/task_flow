import time
from operator import add, sub, mul, floordiv
from flask import Flask, request, jsonify
from task_flow import Graph, InputTask, Task, HyperExecutor


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


class App(Flask):

    def __init__(self):
        super(App, self).__init__(__name__)

        with Graph(name="test") as graph:
            _int1 = InputTask("int1", int0)
            _int2 = InputTask("int2", int0)
            _add = Task("add", add0, _int1, _int2, output=True)
            _sub = Task("sub", sub0, _int1, _int2, output=True)
            _mul = Task("mul", mul0, _int1, _int2, output=True, execute="process")
            _div = Task("div", div0, _int1, _int2, output=True, execute="process")
            _print = Task("print", print0, _add, _sub, _mul, _div)
            self.graph = graph

        self.executor = HyperExecutor(thread_num=2, process_num=2)

        self.add_url_rule("/compute", view_func=self.compute, methods=["POST"])

    def compute(self):
        inputs = request.json
        inputs_map = {"int1": [inputs["x"]], "int2": [inputs["y"]]}
        outputs_map = self.executor.run(self.graph, inputs_map=inputs_map)
        outputs = {
            "x": outputs_map["add"],
            "y": outputs_map["sub"],
            "z": outputs_map["mul"],
            "w": outputs_map["div"]
        }
        return jsonify(outputs)


class HttpServer:

    def __init__(self, host: str, port: int):
        self.app = App()
        self.host = host
        self.port = port

    def run(self):
        self.app.run(host=self.host, port=self.port)


if __name__ == "__main__":
    http_server = HttpServer(host="0.0.0.0", port=8000)
    http_server.run()
