import grpc
import time
from concurrent import futures
from operator import add, sub, mul, floordiv
from task_flow import InputTask, Task, Graph, HyperExecutor
from example.grpc.proto import example_pb2
from example.grpc.proto import example_pb2_grpc


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


class App(example_pb2_grpc.AppServicer):

    def __init__(self):
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

    def Compute(self, inputs, context):
        inputs_map = {"int1": [inputs.x], "int2": [inputs.y]}
        outputs_map = self.executor.run(self.graph, inputs_map=inputs_map)
        outputs = example_pb2.Outputs(
            x=outputs_map["add"],
            y=outputs_map["sub"],
            z=outputs_map["mul"],
            w=outputs_map["div"]
        )
        return outputs


class GrpcServer:

    def __init__(self, host: str, port: int):
        self.app = App()
        self.host = host
        self.port = port

    def run(self):
        server = grpc.server(
            thread_pool=futures.ThreadPoolExecutor(max_workers=3),
            options=[
                ("grpc.max_send_message_length", 32 << 20),
                ("grpc.max_receive_message_length", 32 << 20),
            ],
        )
        example_pb2_grpc.add_AppServicer_to_server(self.app, server)
        server.add_insecure_port("%s:%d" % (self.host, self.port))
        server.start()

        try:
            while True:
                time.sleep(24 * 60 * 60)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    grpc_server = GrpcServer(host="0.0.0.0", port=8000)
    grpc_server.run()
