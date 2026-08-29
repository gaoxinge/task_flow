# V1 重构开发设计

## 1. 目标

V1 在不增加业务功能和语法能力的前提下，对现有项目进行结构重构，主要解决以下问题：

- 使用 `uv` 管理 Python 项目、依赖和开发命令；
- 将 AST 编译、图结构、Executor 执行方式和图输出分层；
- 合并 Inline、Thread、Process Executor 中重复的 DAG 调度逻辑；
- 建立一个最小、稳定、可输出的 `GraphIR`；
- 保持现有计算语义；用户 API 按 V1 设计统一为 `compile`、`transform` 和三个 Executor；
- 重构测试和 benchmark；
- 将旧 `doc/` 和 `example/` 中仍有效的内容整理进根目录 `README.md`。

V1 不追求完整编译器架构，不要求所有阶段都有独立 IR。

## 2. 非目标

以下内容不属于 V1：

- 不新增条件、循环、递归、fork、join 或流式语义；
- 不新增 MPI、Dask、Ray、Celery 等分布式后端；
- 不实现任务缓存、持久化、恢复、重试和运行 Trace；
- 不实现类型推断、复杂图优化和跨节点数据布局；
- 不引入 Semantic IR、Executable Plan IR、Trace IR 等额外中间表示；
- 不扩大当前 Transformer 支持的 Python 语法范围；
- 允许修复现有公开示例或 API 已表达但实现错误的行为，例如单值 `return` 未生成输出、装饰函数 `kwargs` 未绑定输入。这些属于兼容性修复，不视为新增语义。

## 3. V1 总体流程

V1 只保留一个正式中间表示 `GraphIR`，主流程为：

```text
Python Source
    ──[Parser]──> Python AST
    ──[Compiler]──> GraphIR
    ──[Executor]──> Result
```

Executor 是 `GraphIR + Inputs -> Result` 的完整执行阶段。依赖计算、ready queue、任务提交、Future 等待和结果组装都是 Executor 的内部实现，不在 V1 中拆成独立 Planner 或 Backend 层：

```text
Executor(GraphIR, Inputs)
    ├─ 初始化输入、常量和节点依赖状态
    ├─ 调度并执行就绪节点
    ├─ 等待 Inline / Thread / Process 执行结果
    ├─ 解锁后继节点
    └─ 按 GraphIR.outputs 组装 Result
```

GraphIR 还可以进入一条独立于执行主流程的输出支路：

```text
GraphIR ──[Printer]──> dict / JSON / Mermaid / Graphviz
```

V1 不生成 `ExecutablePlanIR`。如果未来出现节点级异构后端选择、分布式资源放置或数据传输规划，再引入 Planner 和 Executable Plan IR。

## 4. 分层设计

### 4.1 API 层

职责：

- 提供 `compile` 和 `transform` 两个装饰器入口；
- 连接 Compiler、Executor 和 Printer；
- 隐藏内部 IR 和执行器初始化细节；
- 让 `compile` 负责 GraphIR 封装，让 `transform` 在其上增加直接执行能力。

API 层不负责：

- AST 节点遍历；
- DAG 调度；
- 线程池或进程池管理；
- Graphviz 渲染细节。

### 4.2 IR 层

IR 层先于 Compiler 定义，因为它是 Compiler 的输出契约，也是 Executor 和 Printer 的共同输入。

职责：

- 表达静态 DAG；
- 保存节点、依赖、图输入和图输出；
- 提供只读遍历和基础校验；
- 作为 Compiler 的构建目标；
- 作为 Executor 和 Printer 的共同输入。

IR 不负责：

- 运行任务；
- 保存 Future；
- 创建线程池或进程池；
- 保存某次运行的状态；
- 直接依赖 Graphviz。

### 4.3 Compiler 层

职责：

- 接收 Python 函数或 Python AST；
- 解析函数参数、常量、赋值、函数调用、算术运算和返回值；
- 维护源码变量到节点 ID 的映射；
- 按 IR 层定义的结构输出 `GraphIR`；
- 对不支持或非法的语法给出明确异常。

Compiler 只构图，不执行函数，也不直接输出 Graphviz。

V1 不再让 AST Visitor 在遍历过程中创建可执行 `Task` 对象并注册到全局 Namespace，而是通过 `GraphBuilder` 显式生成 IR。

### 4.4 Executor 层

V1 不设置独立 Planner 和 Backend 层。公共 Executor API 接收 `CompiledFunction` 与调用参数，内部提取其 `GraphIR` 并转换为 Result。

职责：

- 接收 `CompiledFunction`、位置参数和关键字参数，并读取只读 `GraphIR`；
- 创建一次运行所需的临时状态；
- 计算依赖数并维护 ready queue；
- 执行或提交就绪节点；
- 等待节点完成并解锁后继节点；
- 收集结果和异常；
- 按 Graph 输出顺序返回最终结果；
- 管理线程池或进程池的生命周期。

V1 提供：

- `InlineExecutor`：在当前线程立即执行；
- `ThreadExecutor`：使用 `ThreadPoolExecutor`；
- `ProcessExecutor`：使用 `ProcessPoolExecutor`；

三种 Executor 共享一套 DAG 调度循环，只在“如何提交节点、如何等待结果”上存在差异。V1 可以通过基类的受保护方法消除重复，不需要为这些差异单独建立 Backend 抽象层。

### 4.5 Printer 层

职责：

- 读取 `GraphIR`；
- 输出 dict、JSON、Mermaid 或 Graphviz；
- 显式输出全部节点，包括孤立节点；
- 将节点 ID 和展示 label 分开。

Printer 不修改 Graph，也不参与执行。

## 5. 建议代码结构

```text
task_flow/
├── __init__.py
├── api.py
├── compiler/
│   ├── __init__.py
│   ├── compiler.py
│   ├── transformer.py
│   └── builder.py
├── ir/
│   ├── __init__.py
│   ├── graph.py
│   ├── node.py
│   └── validation.py
├── runtime/
│   ├── __init__.py
│   ├── executor.py
│   └── state.py
└── printer/
    ├── __init__.py
    ├── base.py
    ├── dict.py
    ├── json.py
    ├── mermaid.py
    └── graphviz.py

tests/
├── test_compiler.py
├── test_ir.py
├── test_executor.py
├── test_printer.py
└── test_api.py

benchmarks/
└── benchmark_executor.py

docs/
├── v1/
│   ├── prd.md
│   └── dev.md
└── v2/
    └── ...                  # 保留现有目录，本次不修改

README.md
pyproject.toml
uv.lock
```

如果实现初期文件较少，可以先将 `GraphIR`、`NodeIR` 放在同一个 `ir.py` 中；当代码规模增加后再拆目录。分层边界比文件数量更重要。

`docs/v2/` 作为后续版本设计目录继续保留，但不属于 V1 重构范围。本次不调整其结构，也不修改、迁移或删除其中已有内容。

## 6. 最小 GraphIR

### 6.1 GraphIR

```python
@dataclass(frozen=True)
class GraphIR:
    ir_version: str
    name: str
    nodes: tuple[NodeIR, ...]
    inputs: tuple[tuple[str, str], ...]
    outputs: tuple[str, ...]
    output_kind: str
```

字段含义：

- `ir_version`：GraphIR Schema 版本，V1 固定为 `"1.0"`；
- `name`：图名称；
- `nodes`：按稳定顺序保存所有节点；
- `inputs`：按函数签名顺序保存 `(参数名, 输入节点 ID)`，Executor 据此统一绑定位置参数和关键字参数；
- `outputs`：返回值对应的节点 ID；
- `output_kind`：`none`、`single`、`tuple` 或 `list`，用于恢复 Python 返回结构。

V1 只覆盖项目当前支持的普通位置或关键字参数，不在本次重构中新增仅限关键字参数、仅限位置参数、可变参数或默认参数等语法能力。

### 6.2 NodeIR

```python
@dataclass(frozen=True)
class NodeIR:
    id: str
    kind: str
    name: str
    operation: Callable[..., Any] | None
    dependencies: tuple[str, ...]
    value: Any = None
```

V1 的 `kind` 只需要：

- `input`：图输入，不作为执行节点提交；
- `constant`：常量，不作为执行节点提交；
- `call`：需要执行的函数节点。

Graph 输出直接引用节点 ID，不再创建只负责 echo 的 ReturnTask。

`dependencies` 的顺序就是调用函数时的位置参数顺序，因此 V1 暂时不引入 ValueIR 和端口模型。未来需要关键字参数、多输出端口或流式连接时，再演进为 Value/Port 结构。

### 6.3 为什么 V1 不引入 ValueIR

当前每个 Task 只产生一个 Python 返回值，节点依赖也主要通过位置参数连接。在这个范围内：

```text
Node ID 同时作为该节点输出值的引用
```

已经足够。

例如：

```python
def f(a, b):
    c = a + b
    return c
```

可以表示为：

```json
{
  "ir_version": "1.0",
  "name": "f",
  "inputs": [
    {"name": "a", "node": "input.a"},
    {"name": "b", "node": "input.b"}
  ],
  "outputs": ["call.add"],
  "output_kind": "single",
  "nodes": [
    {
      "id": "input.a",
      "kind": "input",
      "name": "a",
      "dependencies": []
    },
    {
      "id": "input.b",
      "kind": "input",
      "name": "b",
      "dependencies": []
    },
    {
      "id": "call.add",
      "kind": "call",
      "name": "operator.add",
      "dependencies": ["input.a", "input.b"]
    }
  ]
}
```

## 7. GraphIR 的构建方式

当前方式：

```text
Task.__init__
    -> _namespace.top()
    -> graph.add_task()
    -> 修改 parents/children
```

V1 方式：

```text
Compiler
    -> GraphBuilder.add_input()
    -> GraphBuilder.add_constant()
    -> GraphBuilder.add_call()
    -> GraphBuilder.set_outputs()
    -> GraphBuilder.build()
    -> GraphIR
```

`GraphBuilder` 是编译期间的可变对象，`GraphIR` 是构建完成后的不可变对象。

节点 ID 必须在同一 Graph 内唯一，并在相同源码重复编译时保持稳定。V1 建议采用“节点类型 + 语义名称 + 同名出现序号”，例如：

```text
input.a
input.b
call.operator.add.1
call.operator.add.2
constant.1
```

显示名称与节点 ID 分离；Printer 可以显示 `operator.add`，但依赖引用始终使用完整 ID。

这样可以移除运行时全局 `_namespace`，避免并发编译时图上下文相互污染。

## 8. Executor 执行流程

执行开始时，Executor 创建内部临时状态：

```python
@dataclass
class ExecutionState:
    remaining_dependencies: dict[str, int]
    results: dict[str, Any]
    handles: dict[str, Any]
    ready: deque[str]
```

统一执行流程：

```text
1. 将 Graph 输入绑定到 results；
2. 将常量写入 results；
3. Executor 计算 call 节点的依赖数；
4. 将依赖已满足的节点放入 ready；
5. Executor 从 ready 取节点；
6. 根据 dependencies 从 results 获取有序参数；
7. Executor 执行或提交 operation；
8. 节点完成后将结果写入 results；
9. Executor 更新依赖状态并解锁后继节点；
10. 所有 output 节点完成后，根据 `output_kind` 组装并返回 Result：`none -> None`、`single -> value`、`tuple -> tuple`、`list -> list`。
```

Inline、Thread、Process 的区别只在第 7、8 步，依赖维护和结果组装完全共用。

## 9. Executor 设计

V1 使用一个共享执行基类和三个主要实现：

```python
class Executor(ABC):
    def run(
        self,
        function: CompiledFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        graph = function.graph_ir
        # 统一的输入绑定、ready queue、依赖更新和结果组装
        ...

    @abstractmethod
    def _submit(self, function, *args): ...

    @abstractmethod
    def _wait_any(self, handles): ...

    @abstractmethod
    def _result(self, handle): ...
```

具体实现：

```python
class InlineExecutor(Executor):
    # 当前线程执行
    ...


class ThreadExecutor(Executor):
    # ThreadPoolExecutor
    ...


class ProcessExecutor(Executor):
    # ProcessPoolExecutor
    ...
```

这里的 `_submit`、`_wait_any` 和 `_result` 只是 Executor 内部扩展点，不形成独立 Backend 层。等未来接入 Dask、Ray、MPI，且执行底座需要脱离 Executor 独立演进时，再提取 Backend 接口。

## 10. Executor API

Executor 接受 `@compile` 或 `@compile(format=...)` 生成的 `CompiledFunction`，并执行它持有的 `graph_ir`：

```python
from task_flow import compile
from task_flow.runtime import ThreadExecutor


@compile
def add(a, b):
    return a + b


with ThreadExecutor(thread_num=4) as executor:
    result = executor.run(
        add,
        args=(2, 1),
        kwargs={},
    )

assert result == 3
```

建议签名：

```python
class Executor(ABC):
    def run(
        self,
        function: CompiledFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any: ...
```

Executor 不重新解析源码，而是直接执行 `function.graph_ir`。这样 `compile()` 的产物既可打印，也可交给不同 Executor 重复执行。

`SimpleExecutor` 在 V1 中直接删除并替换为 `InlineExecutor`；`HyperExecutor` 也直接删除。两个旧名称都不提供别名或兼容包装。

## 11. Printer 输出规范

### 11.1 dict / JSON

输出内容至少包括：

- IR 版本；
- Graph 名称；
- 输入节点；
- 输出节点；
- 全部节点；
- 节点依赖；
- 节点类型；
- 可稳定定位的 operation 名称；
- 可稳定定位且唯一的节点 ID。

Python callable 不直接写入 JSON，只输出：

```json
{
  "module": "operator",
  "qualname": "add"
}
```

### 11.2 Mermaid / Graphviz

Printer 应：

- 先输出全部节点，再输出边；
- 使用 `NodeIR.id` 作为图节点 ID；
- 使用 `NodeIR.name` 作为展示 label；
- 从 `dependencies` 推导边；
- 对 input、constant、call 使用不同形状；
- 默认输出函数名，而不是统一显示为 `Task`。

Graphviz 是可选输出能力，不应成为核心 Runtime 的强制依赖。

## 12. 用户 API

V1 对外只保留两个核心装饰器：

```text
Python Function
    ──[@compile / @compile(format=...)]──> CompiledFunction(GraphIR)
        ├─[print / format]──> dict / JSON / Mermaid / Graphviz 文本
        └─[Executor.run]──> Result

Python Function
    ──[@transform / @transform(executor=...)]──> TransformedFunction
        └─[直接调用]──> Result
```

二者关系为：

```text
transform = compile + Executor 配置 + 直接调用封装
```

`compile` 的能力更浅，只完成 Python 函数到 GraphIR 的编译和封装；`transform` 复用 `compile`，并进一步绑定 Executor。无括号表示采用默认配置，有括号表示必须显式配置。

### 12.1 compile 装饰器

`compile` 支持两种且仅有两种合法形式。

无括号时使用默认 JSON 打印格式：

```python
from task_flow import compile


@compile
def add(a, b):
    return a + b
```

带括号时必须显式指定打印格式：

```python
@compile(format="mermaid")
def workflow(a, b): ...
```

允许的格式为：

```text
dict
json
mermaid
graphviz
```

空括号非法：

```python
@compile()  # TypeError：使用括号时必须指定 format
```

装饰器的概念签名为：

```python
compile(function) -> CompiledFunction
compile(*, format: GraphFormat) -> Callable[[function], CompiledFunction]
```

装饰发生时执行：

```text
Python Function
    -> inspect.getsource
    -> ast.parse
    -> Compiler
    -> GraphIR
    -> CompiledFunction
```

`CompiledFunction` 至少包含：

```python
class CompiledFunction:
    python_function: Callable[..., Any]
    graph_ir: GraphIR
    default_format: str
```

实现时可以通过只读 property 暴露 `graph_ir`，并在初始化完成后调用 `functools.update_wrapper()`。它应满足以下约束：

- `graph_ir` 编译后只读；
- `default_format` 只控制默认打印方式，不改变 GraphIR；
- 保留原 Python 函数的名称、模块和文档信息；
- 通过 `functools.update_wrapper()` 保留 `__name__`、`__qualname__` 和 `__doc__`；
- 不绑定具体 Executor；
- 不因为打印 GraphIR 而运行用户函数；
- 可以被多个 Executor 重复执行。

`compile` 与 Python 内置 `compile()` 同名是有意的领域 API 设计。建议通过显式导入使用：

```python
from task_flow import compile
```

避免使用通配符导入，减少与 Python 内置名称混淆。

### 12.2 使用 Python print 输出 GraphIR

`CompiledFunction` 实现 Python 的字符串和格式化协议：

```python
class CompiledFunction:
    def __str__(self) -> str: ...

    def __format__(self, format_spec: str) -> str: ...
```

无括号的 `@compile` 默认打印 JSON：

```python
print(add)
```

等价于：

```python
print(format(add, "json"))
```

带参数的 `@compile(format=...)` 会改变 `print()` 的默认格式：

```python
@compile(format="mermaid")
def workflow(a, b): ...


print(workflow)  # Mermaid
```

无论默认格式是什么，都可以通过 Python 内置 `format()` 或格式化字符串临时覆盖：

```python
print(format(add, "dict"))
print(format(add, "json"))
print(format(add, "mermaid"))
print(format(add, "graphviz"))
```

也可以写成：

```python
print(f"{add:dict}")
print(f"{add:json}")
print(f"{add:mermaid}")
print(f"{add:graphviz}")
```

格式约定：

| format spec | 打印内容 |
|---|---|
| 空字符串 | 使用 `CompiledFunction.default_format` |
| `json` | 格式化后的 JSON 文本 |
| `dict` | Python dict 的可读文本表示 |
| `mermaid` | Mermaid flowchart 文本 |
| `graphviz` | Graphviz DOT 源码文本 |

`__format__()` 必须返回字符串，所以 `dict` 格式表示的是可打印的 dict 文本，而不是返回 dict 对象。需要原始 dict 的高级用户应显式使用 Printer，避免把序列化职责放回 GraphIR：

```python
from task_flow.printer import DictPrinter

raw_graph = DictPrinter().print(add.graph_ir)
```

Graphviz 格式在这里输出 DOT 源码，不负责写文件或启动外部渲染程序；因此内置 `print()` 不依赖系统 Graphviz。文件渲染能力继续由 Printer 子模块提供，不作为 V1 的主要用户 API。

不再提供独立的：

```python
run(...)
print_graph(...)
compile_graph(...)
```

### 12.3 使用 Executor 执行 CompiledFunction

`@compile` 和 `@compile(format=...)` 都不绑定执行策略，其产物可以直接交给第 10 节定义的 Executor：

```python
from task_flow import compile
from task_flow.runtime import InlineExecutor


@compile
def add(a, b):
    return a + b


with InlineExecutor() as executor:
    result = executor.run(
        add,
        args=(2, 1),
        kwargs={},
    )

assert result == 3
```

同一个编译结果可以切换 Executor，无需重新编译：

```python
with ThreadExecutor(thread_num=4) as executor:
    thread_result = executor.run(add, args=(2, 1), kwargs={})

with ProcessExecutor(process_num=4) as executor:
    process_result = executor.run(add, args=(2, 1), kwargs={})
```

Executor 内部执行：

```python
graph = compiled_function.graph_ir
```

而不是再次调用 `inspect.getsource()` 或 `ast.parse()`。

### 12.4 transform 装饰器

当用户只想直接执行、不需要显式管理 Executor 时，使用 `@transform`；它默认绑定 `InlineExecutor`：

```python
from task_flow import transform


@transform
def add(a, b):
    return a + b


result = add(2, 1)
assert result == 3
```

显式模式必须通过带参数形式指定 Executor：

```python
@transform(executor="thread", workers=4)
def workflow(a, b): ...


@transform(executor="process", workers=4)
def process_workflow(a, b): ...
```

空括号非法：

```python
@transform()  # TypeError：使用括号时必须指定 executor
```

`@transform(executor="inline")` 合法，但 `workers` 必须为 `None`；Thread 和 Process 模式必须提供合法的 `workers`。

`transform` 的实现语义为：

```text
1. 装饰阶段调用 compile 得到 CompiledFunction；
2. 返回保存 Executor 配置的 TransformedFunction；
3. 每次调用时创建对应 Executor；
4. 执行 Executor.run(compiled, inputs)；
5. 调用结束后关闭 Executor。
```

V1 延续当前“每次调用创建并关闭执行器”的生命周期，避免装饰器长期持有未关闭的线程池或进程池。执行器复用属于后续性能优化，不在 V1 范围内。

`TransformedFunction` 应暴露只读的编译结果：

```python
workflow.compiled
workflow.graph_ir
```

为了保持打印体验一致，`TransformedFunction` 可以将 `__str__` 和 `__format__` 委托给内部 `CompiledFunction`：

```python
print(f"{workflow:mermaid}")
```

V1 只支持：

```text
inline
thread
process
```

不支持 `simple`，也不提供 `SimpleExecutor` 和 `HyperExecutor`。

### 12.5 compile 与 transform 的边界

| 能力 | `@compile` / `@compile(format=...)` | `@transform` / `@transform(executor=...)` |
|---|---:|---:|
| 解析 Python 源码 | 是 | 是，复用 compile |
| 生成 GraphIR | 是 | 是，复用 compile |
| 保存 GraphIR | 是 | 是 |
| 使用 print 输出 GraphIR | 是 | 是，委托 compiled |
| 绑定 Executor | 否 | 是 |
| 装饰后直接调用得到 Result | 否 | 是 |
| 交给其他 Executor 重复执行 | 是 | 可通过 `.compiled` 实现 |

`CompiledFunction` 默认不通过 `__call__` 执行 GraphIR，避免用户误以为它已经绑定执行策略。执行必须显式选择以下方式之一：

```text
Executor.run(compiled, ...)
transform(...)(function)(...)
```

### 12.6 对外导出

`task_flow.__init__` 建议突出导出：

```python
from .api import compile, transform

__all__ = [
    "compile",
    "transform",
]
```

Executor 从运行时子模块导入：

```python
from task_flow.runtime import (
    InlineExecutor,
    ProcessExecutor,
    ThreadExecutor,
)
```

GraphIR 和 Printer 类可以从各自子模块导入，供高级使用，但不作为 README 快速开始中的主要 API。

### 12.7 API 错误约定

V1 至少定义以下明确错误：

- 函数源码无法获取；
- Python 语法不受支持；
- 调用参数与 Graph 输入不匹配；
- `executor` 名称无效；
- `workers` 配置与 Executor 不匹配；
- GraphIR 输出格式无效；
- Process Executor 遇到不可序列化函数或参数。

不支持的格式：

```python
format(add, "unknown")
```

必须产生包含有效格式列表的明确异常。错误应在 API 边界转换为 task-flow 自己的异常类型，并保留原异常作为 `__cause__`。

## 13. 测试设计

### 13.1 Compiler 测试

验证：

- GraphIR 输出固定的 `ir_version="1.0"`；
- 参数生成 input 节点；
- 常量生成 constant 节点；
- 函数调用生成 call 节点；
- 算术表达式依赖顺序正确；
- `None`、单值、tuple 和 list 返回结构及顺序正确；
- 重复调用同一函数时节点 ID 仍然唯一；
- 不支持语法产生明确异常。

测试 GraphIR 结构，不依赖 Graphviz 文件。

### 13.2 Executor 测试

对同一份 GraphIR 分别运行：

- InlineExecutor；
- ThreadExecutor；
- ProcessExecutor。

三种 Executor 必须返回一致结果。

### 13.3 Executor 调度测试

验证：

- 根节点正确进入 ready；
- 多父节点全部完成后才解锁子节点；
- 多个独立节点可同时就绪；
- 非法依赖和循环能够被发现；
- 无关中间结果可以在不再被使用时释放。

### 13.4 Printer 测试

验证：

- dict/JSON 输出稳定；
- 所有节点都被输出；
- 所有依赖边都被输出；
- 节点 ID 与 label 分离；
- Mermaid/Graphviz 使用 snapshot 或文本断言。

### 13.5 API 回归测试

验证两个装饰器场景：

- `@compile` 和 `@compile(format=...)` 只生成 `CompiledFunction` 和 GraphIR，不执行用户函数；
- `print(compiled)` 默认输出 JSON；
- `format(compiled, spec)` 支持 dict、JSON、Mermaid 和 Graphviz；
- 同一个 `CompiledFunction` 能通过三种 Executor 返回一致 Result，包括位置参数和关键字参数；
- `@transform` 默认使用 InlineExecutor，`@transform(executor=...)` 使用显式 Executor；
- `TransformedFunction` 暴露 `.compiled` 和 `.graph_ir`；
- `@compile()` 和 `@transform()` 空括号形式产生明确异常；
- 无效 executor、workers 和 format 产生明确异常；
- README 中的全部 API 示例都作为可运行测试。

## 14. Benchmark 设计

Benchmark 分为两类：

### 14.1 调度开销

- 空函数或轻量函数；
- 不同节点数量；
- 链式 DAG；
- 宽并行 DAG；
- 比较重构前后构图和调度时间。

### 14.2 执行收益

- I/O 模拟任务比较 Inline 与 Thread；
- CPU 模拟任务比较 Inline 与 Process；
- 分别报告编译、调度和实际执行时间；
- 避免只用固定 `sleep(3)` 得出不具代表性的结论。

## 15. uv 和项目配置

V1 以项目当前可运行环境 Python 3.8 为最低兼容基线，不在本次重构中顺带提升最低 Python 版本。文档中的 `tuple[...]`、`dict[...]` 等类型写法属于结构示意；实际代码若需要兼容 Python 3.8，应使用 `typing.Tuple`、`typing.Dict`、`typing.Optional`，或在确认升级最低版本后统一采用新语法。

使用 `pyproject.toml` 统一声明：

- 项目名称与版本；
- Python 最低版本；
- 核心依赖；
- 可选 Graphviz 依赖；
- 开发和测试依赖；
- test、lint、format 配置。

建议命令：

```bash
uv sync --all-groups
uv run pytest
uv run pytest tests/test_executor.py
uv run python -m benchmarks.benchmark_executor
```

Graphviz 建议作为可选依赖：

```toml
[project.optional-dependencies]
graphviz = ["graphviz"]
```

核心的 dict、JSON、Mermaid 输出不依赖系统 Graphviz。

## 16. 文档整理

根据 PRD，V1 完成后：

- 根目录 `README.md` 包含安装、快速开始、核心概念、执行模式、Graph 输出和开发命令；
- 旧 `doc/` 中仍正确的内容合并进 README；
- `example/` 中有价值的示例改写为 README 示例和测试；
- 过期 API 示例不继续保留；
- `docs/v1/` 仅保存本次重构的需求和开发设计。

## 17. 实施步骤

当前代码与 V1 目标的迁移关系如下：

| 当前实现 | V1 目标 | 处理方式 |
|---|---|---|
| `transform/transform.py::Transformer` | Compiler + GraphBuilder | 保留 AST 遍历能力，改为输出 GraphIR |
| `lang/core.py` 中的专用 Task 类型 | Compiler 的操作映射 | 将算术等节点编译为通用 `call` NodeIR，不把运行时 Task 类型带入 IR |
| `runtime/task.py::Task/Graph` | NodeIR + GraphIR | 拆分静态定义与执行状态 |
| 全局 `_namespace` | GraphBuilder 局部状态 | 删除全局构图栈 |
| `SimpleExecutor` | `InlineExecutor` | 直接重命名，不兼容旧名称 |
| `ThreadExecutor` | `ThreadExecutor` | 复用统一执行循环 |
| `ProcessExecutor` | `ProcessExecutor` | 复用统一执行循环 |
| `HyperExecutor` | 无 | 删除 |
| `Graph.show()` | Printer | 从 Runtime 移出 |
| `doc/`、`example/` | README + tests | 合并有效内容，删除过期示例 |
| `setup.py`、`requirements.txt` | `pyproject.toml`、`uv.lock` | 统一项目元数据、依赖和开发命令，并保持 Python 3.8 最低兼容性 |

### 阶段一：建立可验证基线

1. 引入 `pyproject.toml` 和 uv；
2. 将测试迁移到可被 pytest 正常发现的结构；
3. 为现有有效行为补充回归测试；
4. 将 README 最小示例纳入测试。

### 阶段二：建立最小 GraphIR 和编译封装

1. 实现 `NodeIR` 和 `GraphIR`，包含固定 IR 版本和 `output_kind`；
2. 实现 GraphBuilder，并保证节点 ID 唯一；
3. 将 Transformer 改为通过 Builder 输出 GraphIR；
4. 修复单值返回和 kwargs 输入绑定等现有兼容性缺陷；
5. 实现 `CompiledFunction` 与 `compile` 的两种装饰形式；
6. 增加结构校验和编译测试。

### 阶段三：统一 Executor

1. 将现有 `SimpleExecutor` 重命名为 `InlineExecutor`，删除旧名称；
2. 从 Inline、Thread、Process Executor 中提取共享执行循环；
3. 将依赖计数、ready queue 和结果组装统一到 Executor 基类；
4. 实现 Inline、Thread、Process 的内部提交和等待方法；
5. 删除 `HyperExecutor`；
6. 对三个 Executor 运行同一组行为测试。

### 阶段四：拆分 Printer 和完成用户 API

1. 实现 dict 和 JSON Printer；
2. 实现 Mermaid 和 Graphviz DOT Printer；
3. 实现 `CompiledFunction.__str__` 与 `__format__`；
4. 实现 `transform` 的无括号默认形式和带参数显式形式；
5. 将 Graphviz 从 Runtime 移到可选 Printer；
6. 替换原有 `Graph.show()` 实现。

### 阶段五：整理文档与 Benchmark

1. 重写 README；
2. 合并有效 doc/example；
3. 删除或归档过期内容；
4. 增加调度和执行 benchmark；
5. 完成全量测试、格式和构建检查。

## 18. V1 完成标准

满足以下条件即可认为 V1 重构完成：

- Python AST 能编译成节点 ID 稳定且唯一、`ir_version="1.0"`、返回结构明确的最小 GraphIR；
- GraphIR 可输出 dict、JSON、Mermaid 和 Graphviz；
- `compile`、内置 print/format、Executor.run 和 `transform` 有完整回归测试；
- `SimpleExecutor` 已删除，串行执行统一使用 `InlineExecutor`；
- `HyperExecutor` 已删除，不提供兼容别名；
- Inline、Thread、Process 共用同一套 Executor 执行循环；
- 三种 Executor 对相同 GraphIR 返回一致结果；
- Runtime 核心不再强制依赖 Graphviz；
- 构图过程不再依赖全局 `_namespace`；
- `None`、单值、tuple、list 返回结构以及位置参数、kwargs 输入绑定有回归测试保护；
- 项目保持 Python 3.8 最低兼容性；
- pytest 可以自动发现并运行全部测试；
- uv 可以完成环境同步、测试和 benchmark；
- README 中的示例与实际代码保持一致；
- 不引入 PRD 范围外的新执行语义或分布式能力。

## 19. 后续演进预留

V1 完成后可以按需求逐步增加：

```text
GraphIR
    -> Value/Port 模型
    -> 条件和循环 Region
    -> Planner / ExecutionPlanIR
    -> Dask/Ray/MPI Backend
    -> Execution Trace
    -> 流式 Channel
```

这些能力不影响 V1 的主要边界：Compiler 输出 GraphIR，Executor 将 GraphIR 与输入转换为 Result，Printer 只负责输出。Planner 和 Backend 仅在未来复杂度确实需要时再拆分。
