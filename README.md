# dag_workflow

## 简介

自己写着玩的一个简单的工作流框架，支持多节点并行，条件判断等

## 使用

### 安装

1. 创建虚拟环境,推荐使用3.12

2. 安装dag_workflow到虚拟环境

```bash
pip install -e .
```

此时即可在代码中调用dag_workflow包中内容.测试一下example吧!

### todo

新功能:

- [ ] 循环结构

改进:

- [ ] 观察者改为异步
- [ ] 日志改异步
- [ ] 完善observer对各种event识别
- [ ] workflow error event初始化传递context有点不合理

测试:

- [x] 错误测试
- [ ] 条件判断测试
