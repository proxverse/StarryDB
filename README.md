
# 高性能流程挖掘数据库计算层

<img width="612" alt="image" src="https://github.com/user-attachments/assets/1fd2ecb7-f7b7-44fe-b330-956f9640f9dc">

## 简介
本项目旨在实现一个基于 Apache Spark 平台的高性能流程挖掘数据库，通过集成和优化多种技术，提供高效的在线分析处理（OLAP）能力。我们专注于使用 Velox 库优化 Spark 的内存层，提升低基数计算性能，并实现高效的数据连接和分布式数据处理。

## 特性

### Velox Java API
- 实现一套完整的 Velox Java API（包括 plan、expression 和 vectorbatch），参照 Velox 原生类的实现。
- 通过这些 API，实现 Spark 内存层的统一，优化内存管理和数据处理效率。

### 字典执行与延迟解码
- 采用字典执行（dict execution）与延迟解码（later decode）的技术，参照 `RewriteWithGlobalDict` 和 `ExecutorDictManager.scala`。
- 这一策略显著提升了中低基数数据的计算性能。

### 高性能 Range Join
- 实现了一个高性能的 range join，参照 `mergejoin.cpp`。
- 通过优化数据连接算法，加快数据关联操作的速度。

### 推送式内存 Shuffle 与流式 Shuffle
- 引入推送式内存 Shuffle（pushed inmemory shuffle）和流式 Shuffle（steaming shuffle），参照 `shuffle`。
- 这些优化减少了数据在节点间的传输时间，提升了分布式计算的效率。

### 基于 Velox Vector 的列存储缓存
- 利用 Velox vector 实现列存储缓存。
- 通过列式存储和高效的缓存机制，加快数据的读取和处理速度。

### 自动建模以及基于模型的查询优化（待开源）
- 自动建模包括 bucket group、global encoding 和 dict encoding。
- 这一特性旨在进一步提升数据处理效率和查询性能。

## 安装与使用
（todo）
