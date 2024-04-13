package syncer

import (
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
)

type Executer struct {
	taskInfo          *TaskInfo
	config            *configor.Config
	taskManagerClient sqlclient.SqlClient
	reader            sqlclient.SqlClient
	writer            sqlclient.SqlClient
}

func NewExecuter(taskInfo *TaskInfo, config *configor.Config, taskManagerClient sqlclient.SqlClient) (*Executer, error) {
	e := &Executer{
		taskInfo:          taskInfo,
		config:            config,
		taskManagerClient: taskManagerClient,
	}
	reader, err := e.getReader()
	if err != nil {
		logger.Errorf("e.getReader failed:%v", err)
		return nil, err
	}
	e.reader = reader
	logger.Infof("e.getReader success")
	writer, err := e.getWriter()
	if err != nil {
		logger.Errorf("e.getWriter failed:%v", err)
		return nil, err
	}
	e.writer = writer
	logger.Infof("e.getWriter success")
	return e, nil
}

func (e *Executer) getReader() (sqlclient.SqlClient, error) {
	readerKey := fmt.Sprintf("from.%s.%s_%s", e.taskInfo.FromDbType, e.taskInfo.FromApp, e.taskInfo.FromDb)
	reader, err := sqlclient.GetSqlClient(e.taskInfo.FromDbType, e.config, readerKey)
	return reader, err
}

func (e *Executer) getWriter() (sqlclient.SqlClient, error) {
	writerKey := fmt.Sprintf("to.%s.%s_%s", e.taskInfo.ToDbType, e.taskInfo.ToApp, e.taskInfo.ToDb)
	writer, err := sqlclient.GetSqlClient(e.taskInfo.ToDbType, e.config, writerKey)
	return writer, err
}

func (e *Executer) Close() {
	e.reader.Close()
	e.writer.Close()
}
