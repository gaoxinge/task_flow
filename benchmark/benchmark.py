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
def benchmark(executor, graph, _int1, _int2, _add, _minus, _multiply, _divide, _print):
    _int1 = Task("int1", _int1)
    _int2 = Task("int2", _int2)
    _add = Task("add", _add, _int1, _int2)
    _minus = Task("minus", _minus, _int1, _int2)
    _multiply = Task("multiply", _multiply, _int1, _int2)
    _divide = Task("divide", _divide, _int1, _int2)
    _print = Task("print", _print, _add, _minus, _multiply, _divide)
    executor.run(graph)


def benchmark_simple_executor():
    with SimpleExecutor() as executor:
        with Graph(name="test") as graph:
            @generate_sleep_decorator(3)
            def int1_a():
                return 1

            @generate_sleep_decorator(3)
            def int2_a():
                return 2

            @generate_sleep_decorator(3)
            def add_a(a, b):
                return a + b

            @generate_sleep_decorator(3)
            def minus_a(a, b):
                return a - b

            @generate_sleep_decorator(3)
            def multiply_a(a, b):
                return a * b

            @generate_sleep_decorator(3)
            def divide_a(a, b):
                return a // b

            # print is in global scope, so
            # use print_a for local scope
            @generate_sleep_decorator(3)
            def print_a(a, b, c, d):
                return print(a, b, c, d)

            benchmark(executor, graph, int1_a, int2_a, add_a, minus_a, multiply_a, divide_a, print_a)


def benchmark_thread_executor(num):
    with ThreadExecutor(num=num) as executor:
        with Graph(name="test") as graph:
            @generate_sleep_decorator(3)
            def int1_a():
                return 1

            @generate_sleep_decorator(3)
            def int2_a():
                return 2

            @generate_sleep_decorator(3)
            def add_a(a, b):
                return a + b

            @generate_sleep_decorator(3)
            def minus_a(a, b):
                return a - b

            @generate_sleep_decorator(3)
            def multiply_a(a, b):
                return a * b

            @generate_sleep_decorator(3)
            def divide_a(a, b):
                return a // b

            # print is in global scope, so
            # use print_a for local scope
            @generate_sleep_decorator(3)
            def print_a(a, b, c, d):
                return print(a, b, c, d)

            benchmark(executor, graph, int1_a, int2_a, add_a, minus_a, multiply_a, divide_a, print_a)


# pickle and unpickle are used in multiprocess, but
# can not be used for local function and decorator
# in windows
def int1_b():
    time.sleep(3)
    return 1


def int2_b():
    time.sleep(3)
    return 2


def add_b(a, b):
    time.sleep(3)
    return a + b


def minus_b(a, b):
    time.sleep(3)
    return a - b


def multiply_b(a, b):
    time.sleep(3)
    return a * b


def divide_b(a, b):
    time.sleep(3)
    return a // b


def print_b(a, b, c, d):
    time.sleep(3)
    return print(a, b, c, d)


def benchmark_process_executor(num):
    with ProcessExecutor(num=num) as executor:
        with Graph(name="test") as graph:
            benchmark(executor, graph, int1_b, int2_b, add_b, minus_b, multiply_b, divide_b, print_b)


if __name__ == "__main__":
    benchmark_simple_executor()
    benchmark_thread_executor(num=3)
    benchmark_thread_executor(num=4)
    benchmark_process_executor(num=3)
    benchmark_process_executor(num=4)
