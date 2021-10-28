import time
from operator import add, sub, mul, floordiv
from task_flow import InputTask, Task, Graph, SimpleExecutor, ThreadExecutor, ProcessExecutor


def int0(a):
    time.sleep(3)
    return a


def add0(a, b):
    time.sleep(3)
    return add(a, b)


def sub0(a, b):
    time.sleep(3)
    return sub(a, b)


def mul0(a, b):
    time.sleep(3)
    return mul(a, b)


def div0(a, b):
    time.sleep(3)
    return floordiv(a, b)


def print0(*args):
    time.sleep(3)
    return print(*args)


def benchmark(executor, graph):
    start = time.time()
    _int1 = InputTask("int1", int0)
    _int2 = InputTask("int2", int0)
    _add = Task("add", add0, _int1, _int2)
    _sub = Task("sub", sub0, _int1, _int2)
    _mul = Task("mul", mul0, _int1, _int2)
    _div = Task("div", div0, _int1, _int2)
    _print = Task("print", print0, _add, _sub, _mul, _div)
    executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
    print("%.2fs" % (time.time() - start))


def benchmark_simple_executor():
    with SimpleExecutor() as executor:
        with Graph(name="test") as graph:
            benchmark(executor, graph)


def benchmark_thread_executor(thread_num):
    with ThreadExecutor(thread_num=thread_num) as executor:
        with Graph(name="test") as graph:
            benchmark(executor, graph)


def benchmark_process_executor(process_num):
    with ProcessExecutor(process_num=process_num) as executor:
        with Graph(name="test") as graph:
            benchmark(executor, graph)


if __name__ == "__main__":
    benchmark_simple_executor()
    benchmark_thread_executor(thread_num=3)
    benchmark_thread_executor(thread_num=4)
    benchmark_process_executor(process_num=3)
    benchmark_process_executor(process_num=4)
