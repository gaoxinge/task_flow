# 重构

## 环境

- uv
- python库

## 代码结构

```
python ──[parser]──> ast ──[compiler]──> graph ir ──[planner]──> executable ir ──[executor(inline/thread/process)]──> result
                                             |
                                             ──[printer]-> dict/json/mermaid/graphviz
```

## test / benchmark

- 重构优化

## 文档结构

- 把doc/example都集成到readme.md中（docs不用）

## 要求

- 在不填写新功能的基础上，满足上述要求，进行重构
- 输出
  - 代码结构
  - 用户API设计