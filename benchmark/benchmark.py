import time
from operator import add, sub, mul, floordiv
from task_flow import transform


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


def f(a, b):
    _int1 = int0(a)
    _int2 = int0(b)
    _add = add0(_int1, _int2)
    _sub = sub0(_int1, _int2)
    _mul = mul0(_int1, _int2)
    _div = div0(_int1, _int2)
    print0(_add, _sub, _mul, _div)


def benchmark(execute, executor_args):
    start = time.time()
    transform(globals(), execute, executor_args)(f)(2, 1)
    print("%.2fs" % (time.time() - start))


if __name__ == "__main__":
    benchmark("simple", [])
    benchmark("thread", [3])
    benchmark("thread", [4])
    benchmark("process", [3])
    benchmark("process", [4])
