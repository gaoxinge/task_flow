import time
from task_flow import Graph, Task, SimpleExecutor, ThreadExecutor, ProcessExecutor


def generate_sleep_decorator(seconds):
    def sleep_decorator(f):
        def g(*args, **kwargs):
            time.sleep(seconds)
            return f(*args, **kwargs)
        return g
    return sleep_decorator


def time_decorator(f):
    def g(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        print(time.time() - start)
        return result
    return g


@time_decorator
def benchmark(executor, graph, int1, int2, add, minus, multiply, divide, print):
    _int1 = Task("int1", int1)
    _int2 = Task("int2", int2)
    _add = Task("add", add, _int1, _int2)
    _minus = Task("minus", minus, _int1, _int2)
    _multiply = Task("multiply", multiply, _int1, _int2)
    _divide = Task("divide", divide, _int1, _int2)
    _print = Task("print", print, _add, _minus, _multiply, _divide)
    executor.run(graph)


def benchmark_simple_executor():
    with SimpleExecutor() as executor:
        with Graph(name="test") as graph:
            def int1(): return 1
            def int2(): return 2
            from operator import add, sub, mul, floordiv

            _int1 = generate_sleep_decorator(3)(int1)
            _int2 = generate_sleep_decorator(3)(int2)
            _add = generate_sleep_decorator(3)(add)
            _minus = generate_sleep_decorator(3)(sub)
            _multiply = generate_sleep_decorator(3)(mul)
            _divide = generate_sleep_decorator(3)(floordiv)
            _print = generate_sleep_decorator(3)(print)

            benchmark(executor, graph, _int1, _int2, _add, _minus, _multiply, _divide, _print)


def benchmark_thread_executor(num):
    with ThreadExecutor(num=num) as executor:
        with Graph(name="test") as graph:
            def int1(): return 1
            def int2(): return 2
            from operator import add, sub, mul, floordiv

            _int1 = generate_sleep_decorator(3)(int1)
            _int2 = generate_sleep_decorator(3)(int2)
            _add = generate_sleep_decorator(3)(add)
            _minus = generate_sleep_decorator(3)(sub)
            _multiply = generate_sleep_decorator(3)(mul)
            _divide = generate_sleep_decorator(3)(floordiv)
            _print = generate_sleep_decorator(3)(print)

            benchmark(executor, graph, _int1, _int2, _add, _minus, _multiply, _divide, _print)


def _int1():
    time.sleep(3)
    return 1


def _int2():
    time.sleep(3)
    return 2


def _add(a, b):
    time.sleep(3)
    return a + b


def _minus(a, b):
    time.sleep(3)
    return a - b


def _multiply(a, b):
    time.sleep(3)
    return a * b


def _divide(a, b):
    time.sleep(3)
    return a // b


def _print(a, b, c, d):
    time.sleep(3)
    return print(a, b, c, d)


def benchmark_process_executor(num):
    with ProcessExecutor(num=num) as executor:
        with Graph(name="test") as graph:
            benchmark(executor, graph, _int1, _int2, _add, _minus, _multiply, _divide, _print)


if __name__ == "__main__":
    benchmark_simple_executor()
    benchmark_thread_executor(num=3)
    benchmark_thread_executor(num=4)
    benchmark_process_executor(num=3)
    benchmark_process_executor(num=4)
