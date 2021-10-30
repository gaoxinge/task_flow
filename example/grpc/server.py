import grpc
import time
from concurrent import futures
from operator import add, sub, mul, floordiv
from task_flow import HyperExecutor, transform
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


class App(example_pb2_grpc.AppServicer):

    def Compute(self, inputs, context):
        x, y, z, w = f(inputs.x, inputs.y)
        outputs = example_pb2.Outputs(x=x, y=y, z=z, w=w)
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
