package syncer

import (
	"encoding/json"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
)

type Syncer struct {
	condition         string
	mode              string
	taskInfos         []*TaskInfo
	config            *configor.Config
	taskManagerClient sqlclient.SqlClient
	reader            sqlclient.SqlClient
	writer            sqlclient.SqlClient
	stopChan          chan error
}

func NewSyncer(config *configor.Config, condition, mode string) (*Syncer, error) {
	s := &Syncer{
		config:    config,
		condition: condition,
		mode:      mode,
		stopChan:  make(chan error),
	}
	taskManagerClient, err := s.getTaskManagerClient()
	if err != nil {
		logger.Errorf("s.getTaskManagerClient failed:%v", err)
		return nil, err
	}
	s.taskManagerClient = taskManagerClient
	taskInfos, err := s.getWaitedTasks()
	if err != nil {
		return nil, err
	}
	s.taskInfos = taskInfos
	return s, nil
}

func (s *Syncer) getTaskManagerClient() (sqlclient.SqlClient, error) {
	key := "mysql.task_manager"
	taskManagerClient, err := sqlclient.GetSqlClient("mysql", s.config, key)
	return taskManagerClient, err
}

//获取待执行的任务
func (s *Syncer) getWaitedTasks() ([]*TaskInfo, error) {
	taskInfos := make([]*TaskInfo, 0)
	sql := fmt.Sprintf(baseQueryWaitedTasks, s.condition)
	logger.Infof("s.getWaitedTasks.sql is %s", sql)
	tasks, _, err := s.taskManagerClient.Query(sql)
	if err != nil {
		return nil, err
	}
	for _, meta := range tasks {
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			logger.Errorf("taskmeta:%v get,but trans bytes error:%v", meta, err)
			return nil, err
		}
		var task TaskInfo
		err = json.Unmarshal(metaBytes, &task)
		if err != nil {
			logger.Errorf("taskmeta Unmarshal for TaskInfo error ", err)
			return nil, err
		}
		taskInfos = append(taskInfos, &task)

	}
	return taskInfos, nil

}

func (s *Syncer) Run() error {
	if len(s.taskInfos) > 0 {
		taskInfo := s.taskInfos[0]
		e, err := NewExecutor(taskInfo, s)
		defer func() {
			e.Close()
		}()
		if err != nil {
			return err
		}

		e.Run()
		return nil
	}
	return nil
}
