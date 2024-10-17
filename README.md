Spark 平台的模型化 高性能 OLAP 引擎

项目包含:
1. 整套的 velox 的 java api (plan/expression/vectorbatch, 参考 native class相关实现),实现 Spark 内存层统一
2. dict execution + later decode (参考 RewriteWithGlobalDict + ExecutorDictManager.scala), 极大提升中底基数计算性能
3. 高性能 rangejoin 实现 (参考 mergejoin.cpp)
4. pushed (inmemory) shuffle + steaming shuffle (参考 shuffle)
5. 基于 velox vector 的列存 cache
6. 自动建模(bucket group +  global encoding + dict encoding)(todo)
