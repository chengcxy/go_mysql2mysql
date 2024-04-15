# go-mysql2mysql同步工具


### 编译
``` shell
#linux 二进制文件名是go_mysql2mysql
make linux 

#mac 二进制文件名是go_mysql2mysql_mac
make mac
```

### 命令行参数 以linux为例,mac本地跑启动脚本改一下即可

```
cd cmd

#跑全部任务的全量 目标表为空表 未保证数据安全,写入不执行truncate操作
./go_mysql2mysql -c ../config -e test -mode init

#跑全部任务的增量 目标表不为空表自动做数据库的增删 把update改为先删除后批量插入以提升性能
./go_mysql2mysql -c ../config -e test -mode increase


#跑全部任务的全量 目标表为空表 未保证数据安全,写入不执行truncate操作 控制并发数加上-concurrency 参数 100个任务同一时刻起3个任务
./go_mysql2mysql -c ../config -e test -mode init -concurrency 3

#跑全部任务的增量 目标表不为空表自动做数据库的增删 把update改为先删除后批量插入以提升性能 控制并发数加上-concurrency 参数 100个任务同一时刻起3个任务
./go_mysql2mysql -c ../config -e test -mode increase -concurrency 3

#跑单个任务的全量
./go_mysql2mysql -c ../config -e test -mode init -condition="id=124"


#跑单个任务的增量
./go_mysql2mysql -c ../config -e test -mode init -condition="id=124"


#跑部分任务的全量
./go_mysql2mysql -c ../config -e test -mode init -condition="id in(5,124)"


#跑部分任务的增量 默认并发3个
./go_mysql2mysql -c ../config -e test -mode init -condition="id in(5,124)"
```


### 部分日志
```
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:570	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase,wid:12,start-end(10895007,10900007],insertNum:0,updateNum:0,deleteNum:0,affectNum:0
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:487	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase,wid:16 executeIncrease((10915007,10920007])
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:547	taskname:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase worker:16 finished
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:570	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase,wid:16,start-end(10915007,10920007],insertNum:0,updateNum:0,deleteNum:0,affectNum:0
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:487	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase,wid:13 executeIncrease((10960007,10965007])
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:547	taskname:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase worker:13 finished
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:550	taskname:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase closed resultsChan
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:570	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase,wid:13,start-end(10960007,10965007],insertNum:0,updateNum:0,deleteNum:0,affectNum:0
2024-04-15T17:44:51+08:00	INFO	syncer/executor.go:573	taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase finished,insertNum:0,updateNum:0,deleteNum:0,affectNum:0
2024-04-15T17:44:51+08:00	INFO	syncer/syncer.go:123	results is &{taskName:local_dw_mysql_blog_test->local_dw_mysql_blog_test2->increase taskStatus:5 insertNum:0 updateNum:0 deleteNum:0 affectNum:0 err:<nil>}
```
