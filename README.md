# task_flow

## install

### graphviz

- [graphviz](https://graphviz.org/)

### pip

```shell
pip install git+https://github.com/gaoxinge/task_flow
```

## example

### show task graph

```python
from task_flow import Graph, Task


with Graph("test") as graph:
    _int1 = Task("int1", lambda: 1)
    _int2 = Task("int2", lambda: 2)
    _add = Task("add", lambda x, y: x + y, _int1, _int2)
    _minus = Task("minus", lambda x, y: x - y, _int1, _int2)
    _multiply = Task("multiply", lambda x, y: x * y, _int1, _int2)
    _divide = Task("divide", lambda x, y: x // y, _int1, _int2)
    _print = Task("print", lambda x, y, z, w: print(x, y, z, w), _add, _minus, _multiply, _divide)
    graph.show("test.gv")
```

### run task graph by simple executor

```python
from task_flow import Graph, Task, SimpleExecutor


with SimpleExecutor() as executor:
    with Graph("test") as graph:
        _int1 = Task("int1", lambda: 1)
        _int2 = Task("int2", lambda: 2)
        _add = Task("add", lambda x, y: x + y, _int1, _int2)
        _minus = Task("minus", lambda x, y: x - y, _int1, _int2)
        _multiply = Task("multiply", lambda x, y: x * y, _int1, _int2)
        _divide = Task("divide", lambda x, y: x // y, _int1, _int2)
        _print = Task("print", lambda x, y, z, w: print(x, y, z, w), _add, _minus, _multiply, _divide)
        executor.run(graph)
```

### run task graph by thread executor
