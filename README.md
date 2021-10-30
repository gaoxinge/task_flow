# task_flow

## 安装

```
pip install git+https://github.com/gaoxinge/task_flow
```

## 使用

```python
from task_flow import transform

@transform()
def f(a, b):
    return a + b

print(f(2, 1))
```

## 文档

- [文档（过期）](./doc)