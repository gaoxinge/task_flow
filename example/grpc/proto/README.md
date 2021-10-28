# protobuf

## 生成pb文件

```shell
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/example.proto
```