import argparse
import time
from typing import Callable

from task_flow import InlineExecutor, ProcessExecutor, ThreadExecutor, compile


def cpu_work(value):
    total = 0
    for item in range(50_000):
        total += (value + item) % 97
    return total


def benchmark_graph(a, b):
    left = cpu_work(a)
    right = cpu_work(b)
    return left + right


def measure(name: str, factory: Callable, repeats: int) -> None:
    compiled = compile(benchmark_graph)
    start = time.perf_counter()
    with factory() as executor:
        for _ in range(repeats):
            executor.run(compiled, args=(2, 1))
    elapsed = time.perf_counter() - start
    print("%-8s total=%0.4fs average=%0.6fs" % (name, elapsed, elapsed / repeats))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--workers", type=int, default=2)
    options = parser.parse_args()
    measure("inline", InlineExecutor, options.repeats)
    measure("thread", lambda: ThreadExecutor(options.workers), options.repeats)
    measure("process", lambda: ProcessExecutor(options.workers), options.repeats)


if __name__ == "__main__":
    main()
