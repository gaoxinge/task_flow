import grpc
import time
from example.grpc.proto import example_pb2
from example.grpc.proto import example_pb2_grpc

if __name__ == "__main__":
    channel = grpc.insecure_channel("%s:%d" % ("127.0.0.1", 8000), options=[
        ("grpc.max_send_message_length", 32 << 20),
        ("grpc.max_receive_message_length", 32 << 20)
    ])
    grpc.channel_ready_future(channel).result(timeout=5)

    start = time.time()
    stub = example_pb2_grpc.AppStub(channel)
    inputs = example_pb2.Inputs(x=2, y=1)
    outputs = stub.Compute(inputs, timeout=30)
    print("compute consume time: %.2fs, get result: %s" % (time.time() - start, outputs))
