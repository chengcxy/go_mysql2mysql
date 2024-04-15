# go-mysql2mysql同步工具


### 编译
``` shell
#linux 二进制文件名是go_mysql2mysql
make linux 

#mac 二进制文件名是go_mysql2mysql_mac
make mac
```

### 任务配置表 采用mysql存储，方便后续开发后台接口
```
CREATE TABLE `task_def_sync_manager` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '统计任务id',
  `from_app` varchar(50) DEFAULT NULL COMMENT '统计来源业务系统',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT '读取的数据源类型',
  `from_db` varchar(20) DEFAULT NULL COMMENT '来自数据库',
  `from_table` varchar(255) DEFAULT '' COMMENT '统计规则',
  `to_app` varchar(50) DEFAULT NULL COMMENT '写入的业务系统',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT '写入数据源类型',
  `to_db` varchar(20) DEFAULT NULL COMMENT '写入数据库',
  `to_table` varchar(255) DEFAULT NULL COMMENT '写入数据表',
  `params` text COMMENT '增量规则',
  `online_status` int(11) DEFAULT NULL COMMENT '在线状态1在线0下线',
  `task_desc` text COMMENT '统计描述',
  `task_status` varchar(100) DEFAULT NULL COMMENT '任务状态',
  `owner` varchar(100) DEFAULT NULL COMMENT '取数人',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据同步任务-离线';


INSERT INTO `task_def_sync_manager` (`id`, `from_app`, `from_db_type`, `from_db`, `from_table`, `to_app`, `to_db_type`, `to_db`, `to_table`, `params`, `online_status`, `task_desc`, `task_status`, `owner`, `create_time`, `update_time`)
VALUES
	(5, 'local_dw', 'mysql', 'blog', 'test', 'local_dw', 'mysql', 'blog', 'test2', '{\n	\"pk\": {\n		\"src\": \"id\",\n		\"dest\": \"id\"\n	},\n	\"diff_column\": {\n		\"src\": \"update_time\",\n		\"dest\": \"update_time\"\n	},\n	\"worker_num\": 20,\n	\"read_batch\": 5000,\n	\"write_batch\": 500\n}', 1,  '增量导入', '1', '188********',', '2021-03-30 16:13:34', '2024-04-15 17:50:24'),
	(124, 'local_dw', 'mysql', 'blog', 'test', 'local_dw', 'mysql', 'blog', 'test3', '{\n	\"pk\": {\n		\"src\": \"id\",\n		\"dest\": \"id\"\n	},\n	\"diff_column\": {\n		\"src\": \"update_time\",\n		\"dest\": \"update_time\"\n	},\n	\"worker_num\": 20,\n	\"read_batch\": 5000,\n	\"write_batch\": 500\n}', 1,  '增量导入', '1', '188********', '2021-03-30 16:13:34', '2024-04-15 17:50:29');


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
