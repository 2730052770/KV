# KV

## 编译

如果目录下有compile.sh，那么直接运行它。

如果没有，那么直接make。

## 运行

对于KV_multithread_loadbalance，使用

`./selftest NTHREADS NBATCH` （建议NBATCH=12）

对于mica，使用

`./run.sh NTHREADS`

对于其他，使用

`./selftest NBATCH`（建议NBATCH=8）和`./mttest NTHREADS NBATCH`
