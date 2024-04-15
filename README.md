# go-mysql2mysql同步工具


# 编译
``` shell

make linux
```

# 命令行参数

```
cd cmd

#跑全部任务的全量 目标表为空表 未保证数据安全,写入不执行truncate操作
go run main.go -c ../config -e test -mode init

#跑全部任务的增量 目标表不为空表自动做数据库的增删 把update改为先删除后批量插入以提升性能
go run main.go -c ../config -e test -mode increase


#跑全部任务的全量 目标表为空表 未保证数据安全,写入不执行truncate操作 控制并发数加上-concurrency 参数 100个任务同一时刻起3个任务
go run main.go -c ../config -e test -mode init -concurrency 3

#跑全部任务的增量 目标表不为空表自动做数据库的增删 把update改为先删除后批量插入以提升性能 控制并发数加上-concurrency 参数 100个任务同一时刻起3个任务
go run main.go -c ../config -e test -mode increase -concurrency 3

#跑单个任务的全量
go run main.go -c ../config -e test -mode init -condition="id=124"


#跑单个任务的增量
go run main.go -c ../config -e test -mode init -condition="id=124"


#跑部分任务的全量
go run main.go -c ../config -e test -mode init -condition="id in(5,124)"


#跑部分任务的增量 默认并发3个
go run main.go -c ../config -e test -mode init -condition="id in(5,124)"
```
