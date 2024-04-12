package syncer

type Executer struct {
	taskInfo          *TaskInfo
	config            *configor.Config
	taskManagerClient sqlclient.SqlClient
	reader            sqlclient.SqlClient
	writer            sqlclient.SqlClient
}

func NewExecuter(taskInfo *TaskInfo, config *configor.Config, taskManagerClient sqlclient.SqlClient) *Executer {
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
	return e
}

func (e *Executer) getReader() (sqlclient.SqlClient, err) {
	readerKey := fmt.Sprintf("from.%s.%s_%s", e.taskInfo.FromDbType, e.taskInfo.FromApp, e.taskInfo.FromDb)
	reader, err := sqlclient.GetSqlClient(e.taskInfo.FromDbType, e.config, readerKey)
	return reader, err
}

func (e *Executer) getWriter() (sqlclient.SqlClient, err) {
	writerKey := fmt.Sprintf("to.%s.%s_%s", e.taskInfo.ToDbType, e.taskInfo.ToApp, e.taskInfo.ToDb)
	writer, err := sqlclient.GetSqlClient(e.taskInfo.ToDbType, e.config, e.writerKey)
	return writer, err
}

func (e *Executer) Close() {
	e.reader.Close()
	e.writer.Close()
}
