import time
from operator import add, sub, mul, floordiv
from flask import Flask, request, jsonify
from task_flow import HyperExecutor, transform


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


executor = HyperExecutor(thread_num=2, process_num=2)


@transform(globals(), executor=executor)
def f(a, b):
    _int1 = int0(a)
    _int2 = int0(b)
    _add = add0(_int1, _int2)
    _sub = sub0(_int1, _int2)
    _mul = mul0(_int1, _int2)
    _div = div0(_int1, _int2)
    return _add, _sub, _mul, _div


class App(Flask):

    def __init__(self):
        super(App, self).__init__(__name__)
        self.add_url_rule("/compute", view_func=self.compute, methods=["POST"])

    def compute(self):
        inputs = request.json
        x, y, z, w = f(inputs["x"], inputs["y"])
        outputs = {"x": x, "y": y, "z": z, "w": w}
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
