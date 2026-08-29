# task-flow

task-flow 将一小部分 Python 函数编译成静态 `GraphIR`，可打印计算图，也可使用串行、线程池或进程池执行。

## 环境与安装

项目最低支持 Python 3.8，并使用 [uv](https://docs.astral.sh/uv/) 管理环境：

```bash
uv sync --all-groups
```

## 直接执行函数

`@transform` 默认使用 `InlineExecutor`：

```python
from task_flow import transform


@transform
def add(a, b):
    return a + b


assert add(2, b=1) == 3
```

显式使用线程池或进程池时必须指定工作数量：

```python
@transform(executor="thread", workers=4)
def threaded_add(a, b):
    return a + b


@transform(executor="process", workers=4)
def process_add(a, b):
    return a + b
```

`@transform()` 是非法形式；带括号时必须显式传入 `executor`。

## 编译和打印 GraphIR

`@compile` 只编译函数，不绑定执行策略。`print()` 默认输出 JSON：

```python
from task_flow import compile


@compile
def add(a, b):
    return a + b


print(add)
print(format(add, "dict"))
print(format(add, "mermaid"))
print(format(add, "graphviz"))
```

也可以在装饰时改变默认打印格式：

```python
@compile(format="mermaid")
def workflow(a, b):
    return a + b


print(workflow)
```

支持 `dict`、`json`、`mermaid` 和 `graphviz` 四种格式。Graphviz 格式返回 DOT 源码，不要求 Runtime 安装 Graphviz。

`@compile()` 是非法形式；带括号时必须显式传入 `format`。

## 显式选择 Executor

编译结果可以被不同 Executor 重复执行，无需重新编译：

```python
from task_flow import InlineExecutor, ProcessExecutor, ThreadExecutor


with InlineExecutor() as executor:
    assert executor.run(add, args=(2,), kwargs={"b": 1}) == 3

with ThreadExecutor(thread_num=4) as executor:
    assert executor.run(add, args=(2, 1)) == 3

with ProcessExecutor(process_num=4) as executor:
    assert executor.run(add, args=(2, 1)) == 3
```

线程执行适合 I/O 或会释放 GIL 的扩展调用；进程执行适合 CPU 密集型 Python 代码。进程池中的调用函数与参数必须能被 `pickle` 序列化，应优先使用模块顶层命名函数，避免 lambda 和局部函数。

## 完整示例

### 并行计算与 Graphviz 输出

下面的计算图包含四个相互独立的运算节点，线程 Executor 可以并发执行它们：

```python
import time
from operator import add, floordiv, mul, sub

from task_flow import compile, transform


def delayed(operation, x, y):
    time.sleep(0.1)
    return operation(x, y)


def add0(x, y):
    return delayed(add, x, y)


def sub0(x, y):
    return delayed(sub, x, y)


def mul0(x, y):
    return delayed(mul, x, y)


def div0(x, y):
    return delayed(floordiv, x, y)


@transform(executor="thread", workers=4)
def compute_graph(a, b):
    result_add = add0(a, b)
    result_sub = sub0(a, b)
    result_mul = mul0(a, b)
    result_div = div0(a, b)
    return result_add, result_sub, result_mul, result_div


assert compute_graph(2, 1) == (3, 1, 2, 2)


@compile(format="graphviz")
def printable_graph(a, b):
    result_add = a + b
    result_sub = a - b
    result_mul = a * b
    result_div = a // b
    return result_add, result_sub, result_mul, result_div


print(printable_graph)  # 输出 DOT 源码
```

### 集成 HTTP 服务

HTTP 和 gRPC 只是 `TransformedFunction` 的调用入口，调度逻辑仍由 task-flow 管理。安装示例依赖：

```bash
uv sync --extra examples
```

使用 Flask 暴露计算接口：

```python
from flask import Flask, jsonify, request


app = Flask(__name__)


@app.post("/compute")
def compute():
    inputs = request.get_json()
    x, y, z, w = compute_graph(inputs["x"], inputs["y"])
    return jsonify({"x": x, "y": y, "z": z, "w": w})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
```

请求示例：

```python
import requests


response = requests.post(
    "http://127.0.0.1:8000/compute",
    json={"x": 2, "y": 1},
    timeout=30,
)
assert response.json() == {"x": 3, "y": 1, "z": 2, "w": 2}
```

### 集成 gRPC 服务

先定义 `compute.proto`：

```proto
syntax = "proto3";
package example;

message Inputs {
    int32 x = 1;
    int32 y = 2;
}

message Outputs {
    int32 x = 1;
    int32 y = 2;
    int32 z = 3;
    int32 w = 4;
}

service App {
    rpc Compute (Inputs) returns (Outputs);
}
```

生成 Python 代码：

```bash
uv run python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  compute.proto
```

在生成的服务接口中调用同一个 `compute_graph`：

```python
from concurrent import futures

import grpc
import compute_pb2
import compute_pb2_grpc


class App(compute_pb2_grpc.AppServicer):
    def Compute(self, inputs, context):
        x, y, z, w = compute_graph(inputs.x, inputs.y)
        return compute_pb2.Outputs(x=x, y=y, z=z, w=w)


server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
compute_pb2_grpc.add_AppServicer_to_server(App(), server)
server.add_insecure_port("0.0.0.0:8000")
server.start()
server.wait_for_termination()
```

## V1 编译范围

V1 支持普通位置或关键字参数、常量、简单赋值、普通函数调用、`+`、`-`、`*`、`/`、`//`，以及 `None`、单值、tuple 和 list 返回。条件、循环、递归、可变参数、默认参数和关键字调用参数尚未纳入 V1。

总体流程：

```text
Python Source -> Python AST -> GraphIR -> Executor -> Result
                                  |
                                  +-> Printer -> dict / JSON / Mermaid / Graphviz
```

V1 不包含 Planner、ExecutablePlanIR、Dask、Ray 或 MPI 后端。设计说明见 [`docs/v1/dev.md`](docs/v1/dev.md)。

## 开发

```bash
uv run pytest
uv run python -m benchmarks.benchmark_executor
```
