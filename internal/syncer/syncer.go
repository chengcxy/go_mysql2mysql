package syncer

import (
	"encoding/json"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
	"github.com/chengcxy/go_mysql2mysql/internal/roboter"
)

type Syncer struct {
	condition         string
	mode              string
	taskInfos         []*TaskInfo
	config            *configor.Config
	taskManagerClient sqlclient.SqlClient
	reader            sqlclient.SqlClient
	writer            sqlclient.SqlClient
	concurrency       int
	robot             roboter.Roboter
}

func NewSyncer(config *configor.Config, condition, mode string, concurrency int) (*Syncer, error) {
	s := &Syncer{
		config:      config,
		condition:   condition,
		mode:        mode,
		concurrency: concurrency,
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
	s.robot = roboter.GetRoboter(config)
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

func (s *Syncer) worker(wid int, taskChan chan *TaskInfo, results chan *Result, dones chan int) {
	defer func() {
		dones <- wid
	}()
	for ti := range taskChan {
		e, _ := NewExecutor(ti, s)
		r := e.Run()
		results <- r
	}

}

func (s *Syncer) produceTasks() chan *TaskInfo {
	taskChan := make(chan *TaskInfo)
	go func() {
		defer func() {
			close(taskChan)
		}()
		for _, taskInfo := range s.taskInfos {
			logger.Infof("taskInfo: %+v send to taskChan", taskInfo)
			taskChan <- taskInfo
		}
	}()
	return taskChan

}

func (s *Syncer) Run() error {
	if len(s.taskInfos) > 0 {
		logger.Infof("sync waited %d tasks", len(s.taskInfos))
		taskChan := s.produceTasks()
		results := make(chan *Result)
		dones := make(chan int, s.concurrency)
		for i := 0; i < s.concurrency; i++ {
			go s.worker(i, taskChan, results, dones)
		}
		go func(results chan *Result, dones chan int) {
			for i := 0; i < s.concurrency; i++ {
				<-dones
			}
			close(results)
		}(results, dones)
		for r := range results {
			if r.err != nil {
				return r.err
			}
			logger.Infof("results is %+v", r)
		}
		return nil
	}
	s.robot.SendMsg("go_mysql2mysql task finished ")
	return nil
}
