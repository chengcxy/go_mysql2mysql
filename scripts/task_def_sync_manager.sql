CREATE TABLE `task_def_sync_manager` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '任务id',
  `from_app` varchar(50) DEFAULT NULL COMMENT '来源业务系统',
  `from_db_type` varchar(50) DEFAULT NULL COMMENT '读取的数据源类型',
  `from_db` varchar(20) DEFAULT NULL COMMENT '来自数据库',
  `from_table` varchar(255) DEFAULT '' COMMENT '读取的数据表',
  `to_app` varchar(50) DEFAULT NULL COMMENT '写入的业务系统',
  `to_db_type` varchar(20) DEFAULT NULL COMMENT '写入数据源类型',
  `to_db` varchar(20) DEFAULT NULL COMMENT '写入数据库',
  `to_table` varchar(255) DEFAULT NULL COMMENT '写入数据表',
  `params` text COMMENT '参数',
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
