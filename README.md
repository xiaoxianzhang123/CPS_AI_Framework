# dag_workflow

## 简介

自己写着玩的一个简单的工作流框架，支持多节点并行，条件判断等

## 使用

### 安装

1. 创建虚拟环境,推荐使用3.12

2. 安装dag_workflow到虚拟环境

```
pip install -e .
```

此时即可在代码中调用dag_workflow包中内容

3.测试一下example吧!

### todo

新功能:

- [X] 循环结构

改进:

- [X] 观察者改为异步
- [X] 日志改异步
- [X] 完善observer对各种event识别
- [X] workflow error event初始化传递context有点不合理

测试:

- [X] 错误测试
- [X] 条件判断测试
