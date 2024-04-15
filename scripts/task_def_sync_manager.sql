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
  `write_mode` varchar(50) DEFAULT NULL COMMENT '写入模式replace/insert/append/overwrite',
  `task_desc` text COMMENT '统计描述',
  `task_status` varchar(100) DEFAULT NULL COMMENT '任务状态',
  `owner` varchar(100) DEFAULT NULL COMMENT '取数人',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据同步任务-离线';


