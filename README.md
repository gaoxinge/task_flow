# task_flow

## 安装

```
pip install git+https://github.com/gaoxinge/task_flow
```

## 使用

```python
from operator import add
from task_flow import InputTask, Task, Graph, HyperExecutor

with HyperExecutor(thread_num=2, process_num=2) as executor:
    with Graph(name="test") as graph:
        _int1 = InputTask("int1", int)
        _int2 = InputTask("int2", int)
        _add = Task("add", add, _int1, _int2, output=True, execute="process")
        outputs_map = executor.run(graph, inputs_map={"int1": [2], "int2": [1]})
        print(outputs_map)
```

## 文档

- [文档](./doc)