package syncer

import (
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
)

type Syncer struct {
	condition         string
	taskInfos         []*TaskInfo
	config            *configor.Config
	taskManagerClient sqlclient.SqlClient
	reader            sqlclient.SqlClient
	writer            sqlclient.SqlClient
}

func NewSyncer(config *configor.Config, condition string) (*Syncer, error) {
	s := &Syncer{
		config:    config,
		condition: condition,
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

func (s *Syncer) getTaskManagerClient() (sqlclient.SqlClient, err) {
	key := "mysql.task_manager"
	taskManagerClient, err := sqlclient.GetSqlClient("mysql", s.config, key)
	return taskManagerClient, err
}

//获取待执行的任务
func (s *Syncer) getWaitedTasks() ([]*TaskInfo, error) {
	taskInfos := make([]*TaskInfo, 0)
	sql := fmt.Sprinf(baseQueryWaitedTasks, s.condition)
	logger.Infof("s.getWaitedTasks.sql is %s", sql)
	tasks, _, err := s.taskManagerClient.Query(sql)
	if err != nil {
		return nil, err
	}
	for _, meta := range tasks {
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			log.Errorf("taskmeta:%v get,but trans bytes error:%v", meta, err)
			return nil, err
		}
		var task TaskInfo
		err = json.Unmarshal(metaBytes, &task)
		if err != nil {
			log.Errorf("taskmeta Unmarshal for TaskInfo error ", err)
			return nil, err
		}
		taskInfos = append(taskInfos, &task)

	}
	return taskInfos, nil

}

func (s *Syncer) ExecuteTask(taskInfo *TaskInfo) (int, int, error) {
	e := NewExecuter(taskInfo, s.config, s.taskManagerClient)
	defer func() {
		e.Close()
	}()
}

func (s *Syncer) Run() {
	if len(s.taskInfos) > 0 {
		taskInfo := s.taskInfos[0]
		e := NewExecuter(taskInfo, s.config, s.taskManagerClient)
		defer func() {
			e.Close()
		}()

	}
}
