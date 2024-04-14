package syncer

import (
	"fmt"
	"errors"
	"encoding/json"
	"strconv"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
	
)

type Executer struct {
	syncer 			  *Syncer				//父对象
	taskInfo          *TaskInfo 			//任务信息
	taskName          string				//任务名
	reader            sqlclient.SqlClient	//读取客户端
	writer            sqlclient.SqlClient	//写入客户端
	srcPk 			  string				//读取表的主键字段
	srcCheckKey       string                //读取表的检测字段
	destPk	          string				//写入表的主键字段
	destCheckKey      string                //写入表的检测字段
	workerNum  		  int    				//协程数
	readBatch         int      				//读取表batch
	writeBatch        int      				//写入表批次
	taskStatus		  int					//任务状态
	columnMapping     map[string]string
}


type Result struct{
	taskName 	string
	taskStatus 	int
	insertNum 	int64
	updateNum 	int64
	deleteNum 	int64
	err        	error
}

type TaskResult struct{
	taskName 	string
	start 		int64
	end   		int64
	wid   		int
	insertNum 	int64
	updateNum 	int64
	deleteNum 	int64
	err        	error
}
func NewExecuter(taskInfo *TaskInfo,syncer *Syncer) (*Executer, error) {
	e := &Executer{
		taskInfo:          taskInfo,
		syncer:syncer,
	}
	e.taskName = e.getTaskName()
	logger.Infof("e.taskName is:%s", e.taskName)
	reader, err := e.getReader()
	if err != nil {
		logger.Errorf("e.getReader failed:%v", err)
		return nil, err
	}
	e.reader = reader
	logger.Infof("e.getReader success")
	
	columnMapping,err := e.getReaderColumns()
	if err != nil {
		logger.Errorf("e.getReaderColumns failed:%v", err)
		return nil, err
	}
	e.columnMapping = columnMapping

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
	reader, err := sqlclient.GetSqlClient(e.taskInfo.FromDbType, e.syncer.config, readerKey)
	return reader, err
}

func (e *Executer) getWriter() (sqlclient.SqlClient, error) {
	writerKey := fmt.Sprintf("to.%s.%s_%s", e.taskInfo.ToDbType, e.taskInfo.ToApp, e.taskInfo.ToDb)
	writer, err := sqlclient.GetSqlClient(e.taskInfo.ToDbType, e.syncer.config, writerKey)
	return writer, err
}

func (e *Executer)getTaskName()string{
	fromInfo :=  fmt.Sprintf("%s_%s_%s_%s",e.taskInfo.FromApp,e.taskInfo.FromDbType,e.taskInfo.FromDb,e.taskInfo.FromTable)
	toInfo :=  fmt.Sprintf("%s_%s_%s_%s",e.taskInfo.ToApp,e.taskInfo.ToDbType,e.taskInfo.ToDb,e.taskInfo.ToTable)
	taskName := fmt.Sprintf("%s->%s->%s",fromInfo,toInfo,e.syncer.mode)
	return taskName
	
}

func (e *Executer)parseParams()error{
	var f map[string]interface{}
	err := json.Unmarshal([]byte(e.taskInfo.Params), &f)
	if err != nil {
		return errors.New(fmt.Sprintf("task.params Json Unmarshal error:%v", err))
	}
	e.workerNum = int(f["worker_num"].(float64))
	e.readBatch = int(f["read_batch"].(float64))
	e.writeBatch = int(f["write_batch"].(float64))
	pkMap := f["pk"].(map[string]interface{})
	e.srcPk = pkMap["src"].(string)
	e.destPk = pkMap["dest"].(string)
	diffColumnMap := f["diff_column"].(map[string]interface{})
	e.srcCheckKey = diffColumnMap["src"].(string)
	e.destCheckKey = diffColumnMap["dest"].(string)
	return nil
}

func (e *Executer) Close() {
	e.reader.Close()
	e.writer.Close()
}



func (e *Executer)getMinMaxInit()(int64,int64,error){
	sql := fmt.Sprintf(baseQueryMinMax,e.srcPk,e.srcPk,e.taskInfo.FromDb,e.taskInfo.FromTable)
	logger.Infof("e.getMinMaxInit is %s",sql)
	datas,_,err := e.reader.Query(sql)
	if err != nil{
		return int64(0),int64(0),err
	}
	strMinId := datas[0]["min_id"]
	strMaxId := datas[0]["max_id"]
	start,err := strconv.Atoi(strMinId)
	if err != nil{
		return int64(0),int64(0),err
	}
	end,err := strconv.Atoi(strMaxId)
	if err != nil{
		return int64(0),int64(0),err
	}
	return int64(start),int64(end),nil
}

func(e *Executer)getReaderColumns()(map[string]string,error){
	tm,err := e.reader.GetTableMeta(e.taskInfo.FromApp,e.taskInfo.FromDb,e.taskInfo.FromTable)
	if err != nil{
		return nil,err
	}
	columnMapping := make(map[string]string)
	for _,key := range tm.Fields{
		columnMapping[key] = key
	}
	return columnMapping,nil
}

func (e *Executer)getMinMaxIncrease()(int64,int64,error){
	minId,maxId,err := e.getMinMaxInit()
	if err != nil{
		return int64(0),int64(0),err
	}
	sql := fmt.Sprintf(baseQueryMinMax,e.destPk,e.destPk,e.taskInfo.ToDb,e.taskInfo.ToTable)
	logger.Infof("e.getMinMaxInit is %s",sql)
	datas,_,err := e.reader.Query(sql)
	if err != nil{
		return int64(0),int64(0),err
	}
	strMinId := datas[0]["min_id"]
	strMaxId := datas[0]["max_id"]
	start,err := strconv.Atoi(strMinId)
	if err != nil{
		return int64(0),int64(0),err
	}
	end,err := strconv.Atoi(strMaxId)
	if err != nil{
		return int64(0),int64(0),err
	}
	
	if int64(start) < minId{
		minId = int64(start)
	}
	if int64(end) > maxId{
		maxId = int64(end)
	}
	return minId,maxId,nil
}

func (e *Executer)produceTaskParams(minId,maxId int64)(chan *TaskParams){
	tasksChan := make(chan *TaskParams)
	go func(){
		defer func(){
			close(tasksChan)
		}()
		for minId < maxId{
			end := minId + int64(e.readBatch)
			if end >= maxId{
				end = maxId
			}
			tp := &TaskParams{
				start:minId,
				end:end,
			}
			tasksChan <- tp
			minId = end
		}

	}()
	return tasksChan
}

func(e *Executer)executeInit(wid int,tp *TaskParams)*TaskResult{
	sql := fmt.Sprintf(baseQuerySrcInit,e.taskInfo.FromDb,e.taskInfo.FromTable,e.srcPk,tp.start,e.srcPk,tp.end)
	logger.Infof("taskName:%s,wid:%d queryInitData sql:%s",e.taskName,wid,sql)
	datas,columns,err := e.reader.Query(sql)
	//如果需要列转换 对datas要遍历处理一次
	insertNum,err := e.writer.Write("insert",e.taskInfo.ToDb,e.taskInfo.ToTable,datas,columns,e.writeBatch)
	r := &TaskResult{
		taskName:e.taskName,
		wid:wid,
		start:tp.start,
		end:tp.end,
		insertNum:insertNum,
		err:err,
	}
	logger.Infof("taskName:%s,wid:%d executeInit((%d,%d])",e.taskName,wid,tp.start,tp.end)
	return r
}

func (e *Executer)workerInit(wid int,tasksChan chan *TaskParams,resultsChan chan *TaskResult,dones chan int){
	defer func(){
		dones <- wid
	}()
	for tp := range tasksChan{
		r := e.executeInit(wid,tp)
		resultsChan <- r
	}
}


func(e *Executer)executeIncrease(wid int,tp *TaskParams)*TaskResult{
	r := &TaskResult{
		taskName:e.taskName,
		wid:wid,
		start:tp.start,
		end:tp.end,
		err:nil,
	}
	logger.Infof("taskName:%s,wid:%d executeIncrease((%d,%d])",e.taskName,wid,tp.start,tp.end)
	return r
}


func (e *Executer)workerIncrease(wid int,tasksChan chan *TaskParams,resultsChan chan *TaskResult,dones chan int){
	defer func(){
		dones <- wid
	}()
	for tp := range tasksChan{
		r := e.executeIncrease(wid,tp)
		resultsChan <- r
	}
}


func(e *Executer)Run()*Result{
	//parseParmas valid json
	var err error
	err = e.parseParams()
	if err != nil{
		e.taskStatus = PARSEPARAMSERROR
		result := &Result{
			taskName:e.taskName,
			taskStatus:e.taskStatus,
			err:err,
		}
		return result
	}
	//get minid maxId
	var minId int64 
	var maxId int64
	if e.syncer.mode == "init"{
		minId,maxId,err = e.getMinMaxInit()
	}else{
		minId,maxId,err = e.getMinMaxIncrease()
	}
	if err != nil{
		e.taskStatus = GETMINMAXERROR
		result := &Result{
			taskName:e.taskName,
			taskStatus:e.taskStatus,
			err:err,
		}
		return result
	}
	tasksChan := e.produceTaskParams(minId,maxId)
	resultsChan := make(chan *TaskResult)
	dones := make(chan int,e.workerNum)
	for i:=0;i<e.workerNum;i++{
		if e.syncer.mode == "init"{
			go e.workerInit(i,tasksChan,resultsChan,dones)
		}else{
			go e.workerIncrease(i,tasksChan,resultsChan,dones)
		}
	}
	go func(resultsChan chan *TaskResult,dones chan int ){
		for  i:=0;i<e.workerNum;i++{
			wid := <- dones
			logger.Infof("taskname:%s worker:%d finished",e.taskName,wid)
		}
		close(resultsChan)
		logger.Infof("taskname:%s closed resultsChan",e.taskName)
	}(resultsChan,dones)
	result := &Result{
		taskName:e.taskName,
		insertNum:	int64(0),
		updateNum:int64(0),
		deleteNum:int64(0),
	}
	for r := range resultsChan{
		if r.err != nil{
			result.err = err
			result.taskStatus = FAILED
			logger.Errorf("taskName:%s,error:%v",e.taskName,r.err)
			return result
		}else{
			result.insertNum += r.insertNum
			result.updateNum += r.updateNum
			result.deleteNum += r.deleteNum
			logger.Infof("taskName:%s,wid:%d,start-end(%d,%d],insertNum:%d,updateNum:%d,deleteNum:%d",e.taskName,r.wid,r.start,r.end,r.insertNum,r.updateNum,r.deleteNum)
		}
	}
	logger.Infof("taskName:%s finished,insertNum:%d,updateNum:%d,deleteNum:%d",e.taskName,result.insertNum,result.updateNum,result.deleteNum)
	result.err = nil
	result.taskStatus = SUCCESS
	return result
}
