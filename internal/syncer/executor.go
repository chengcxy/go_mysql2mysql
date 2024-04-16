package syncer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/sqlclient"
	"strconv"
	"strings"
	"sync"
)

var lock sync.Mutex

type Executor struct {
	syncer              *Syncer             //父对象
	taskInfo            *TaskInfo           //任务信息
	taskName            string              //任务名
	reader              sqlclient.SqlClient //读取客户端
	writer              sqlclient.SqlClient //写入客户端
	srcPk               string              //读取表的主键字段
	srcCheckKey         string              //读取表的检测字段
	destPk              string              //写入表的主键字段
	destCheckKey        string              //写入表的检测字段
	workerNum           int                 //协程数
	readBatch           int                 //读取表batch
	writeBatch          int                 //写入表批次
	taskStatus          int                 //任务状态
	columnMapping       map[string]string
	baseDeleteDestSql   string
	baseQuerySrcByPkSql string
}

type Result struct {
	taskName   string
	taskStatus int
	insertNum  int64
	updateNum  int64
	deleteNum  int64
	affectNum  int64
	err        error
}

type TaskResult struct {
	taskName  string
	start     int64
	end       int64
	wid       int
	insertNum int64
	updateNum int64
	deleteNum int64
	affectNum int64
	err       error
}

func NewExecutor(taskInfo *TaskInfo, syncer *Syncer) (*Executor, error) {
	e := &Executor{
		taskInfo: taskInfo,
		syncer:   syncer,
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

	columnMapping, err := e.getReaderColumns()
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

func (e *Executor) getReader() (sqlclient.SqlClient, error) {
	readerKey := fmt.Sprintf("from.%s.%s_%s", e.taskInfo.FromDbType, e.taskInfo.FromApp, e.taskInfo.FromDb)
	reader, err := sqlclient.GetSqlClient(e.taskInfo.FromDbType, e.syncer.config, readerKey)
	return reader, err
}

func (e *Executor) getWriter() (sqlclient.SqlClient, error) {
	writerKey := fmt.Sprintf("to.%s.%s_%s", e.taskInfo.ToDbType, e.taskInfo.ToApp, e.taskInfo.ToDb)
	writer, err := sqlclient.GetSqlClient(e.taskInfo.ToDbType, e.syncer.config, writerKey)
	return writer, err
}

func (e *Executor) getTaskName() string {
	fromInfo := fmt.Sprintf("%s_%s_%s_%s", e.taskInfo.FromApp, e.taskInfo.FromDbType, e.taskInfo.FromDb, e.taskInfo.FromTable)
	toInfo := fmt.Sprintf("%s_%s_%s_%s", e.taskInfo.ToApp, e.taskInfo.ToDbType, e.taskInfo.ToDb, e.taskInfo.ToTable)
	taskName := fmt.Sprintf("%s->%s->%s", fromInfo, toInfo, e.syncer.mode)
	return taskName

}

func (e *Executor) parseParams() error {
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
	e.baseDeleteDestSql = fmt.Sprintf(baseDeleteDestSql, e.taskInfo.ToDb, e.taskInfo.ToTable, e.destPk)
	e.baseQuerySrcByPkSql = fmt.Sprintf(baseQuerySrcByPkSql, e.taskInfo.FromDb, e.taskInfo.FromTable, e.srcPk)
	return nil
}

func (e *Executor) Close() {
	e.reader.Close()
	e.writer.Close()
}

func (e *Executor) getMinMaxInit() (int64, int64, error, bool) {
	var srcEmpty bool
	sql := fmt.Sprintf(baseQueryMinMax, e.srcPk, e.srcPk, e.taskInfo.FromDb, e.taskInfo.FromTable)
	logger.Infof("e.getMinMaxInit is %s", sql)
	datas, _, _ := e.reader.Query(sql)
	strMinId := datas[0]["min_id"]
	if strMinId == "NULL" {
		strMinId = "0"
		srcEmpty = true
	}
	strMaxId := datas[0]["max_id"]
	if strMaxId == "NULL" {
		strMaxId = "0"
	}
	start, err := strconv.Atoi(strMinId)
	if err != nil {
		return int64(0), int64(0), err, srcEmpty
	}
	end, err := strconv.Atoi(strMaxId)
	if err != nil {
		return int64(0), int64(0), err, srcEmpty
	}
	return int64(start), int64(end), nil, srcEmpty
}

func (e *Executor) getReaderColumns() (map[string]string, error) {
	tm, err := e.reader.GetTableMeta(e.taskInfo.FromApp, e.taskInfo.FromDb, e.taskInfo.FromTable)
	if err != nil {
		return nil, err
	}
	columnMapping := make(map[string]string)
	for _, key := range tm.Fields {
		columnMapping[key] = key
	}
	return columnMapping, nil
}

func (e *Executor) getMinMaxIncrease() (int64, int64, error, bool) {
	minId, maxId, err, srcEmpty := e.getMinMaxInit()
	if err != nil {
		return int64(0), int64(0), err, srcEmpty
	}
	sql := fmt.Sprintf(baseQueryMinMax, e.destPk, e.destPk, e.taskInfo.ToDb, e.taskInfo.ToTable)
	logger.Infof("e.getMinMaxIncrease is %s", sql)
	datas, _, _ := e.reader.Query(sql)
	strMinId := datas[0]["min_id"]
	if strMinId == "NULL" {
		strMinId = "0"
	}
	start, err := strconv.Atoi(strMinId)
	if err != nil {
		return int64(0), int64(0), err, srcEmpty
	}
	strMaxId := datas[0]["max_id"]
	if strMaxId == "NULL" {
		strMaxId = "0"
	}
	end, err := strconv.Atoi(strMaxId)
	if err != nil {
		return int64(0), int64(0), err, srcEmpty
	}

	if int64(start) < minId {
		minId = int64(start)
	}
	if int64(end) > maxId {
		maxId = int64(end)
	}
	return minId, maxId, nil, srcEmpty
}

func (e *Executor) getNextPk(start int64, srcEmpty bool) (int64, error) {
	var queryNextSql string
	var datas []map[string]string
	if srcEmpty {
		queryNextSql = fmt.Sprintf(baseQueryNextId, e.srcPk, e.srcPk, e.taskInfo.FromDb, e.taskInfo.FromTable, e.srcPk, start, e.readBatch, e.srcPk)
		logger.Infof("getNextPk is %s", queryNextSql)
		datas, _, _ = e.writer.Query(queryNextSql)
	} else {
		queryNextSql = fmt.Sprintf(baseQueryNextId, e.srcPk, e.srcPk, e.taskInfo.FromDb, e.taskInfo.FromTable, e.srcPk, start, e.readBatch, e.srcPk)
		logger.Infof("getNextPk is %s", queryNextSql)
		datas, _, _ = e.reader.Query(queryNextSql)
	}
	nextPk := datas[0]["endPk"]
	//没有比start更大的
	if nextPk == "NULL" {
		return start, nil
	}
	end, err := strconv.Atoi(nextPk)
	if err != nil {
		return int64(0), err
	}
	return int64(end), nil

}

func (e *Executor) produceTaskParams(minId, maxId int64, srcEmpty bool) chan *TaskParams {
	tasksChan := make(chan *TaskParams)
	go func() {
		defer func() {
			close(tasksChan)
		}()
		for minId < maxId {
			end, err := e.getNextPk(minId, srcEmpty)
			if err != nil {
				return
			}
			if end >= maxId {
				end = maxId
			}
			tp := &TaskParams{
				start: minId,
				end:   end,
			}
			tasksChan <- tp
			minId = end
		}

	}()
	return tasksChan
}

func (e *Executor) processSrcDatas(sql string) ([]map[string]string, []string, error) {
	datas, columns, err := e.reader.Query(sql)
	if err != nil {
		return datas, columns, err
	}
	//TODO 考虑到未来有转换列需求如果需要列转换 对datas要遍历处理一次
	// newDatas := make([]map[string]string,len(datas))
	// for index,data := range datas{
	// 	item := make(map[string]string,0)
	// 	for k,v := range data{
	// 		item[e.columnMapping[k]] = v
	// 	}
	// 	newDatas[index] = item
	// }
	// writeColumns := make([]string,len(columns))
	// for index,column := range columns{
	// 	writeColumns[index] = e.columnMapping[column]
	// }
	return datas, columns, nil

}

//全量分段读取-分段写入
func (e *Executor) executeInit(wid int, tp *TaskParams) *TaskResult {
	columns := make([]string, 0)
	for k, _ := range e.columnMapping {
		columns = append(columns, k)
	}
	columnStr := strings.Join(columns, ",")
	sql := fmt.Sprintf(baseQuerySrcInit, columnStr, e.taskInfo.FromDb, e.taskInfo.FromTable, e.srcPk, tp.start, e.srcPk, tp.end)
	logger.Infof("taskName:%s,wid:%d queryInitData sql:%s", e.taskName, wid, sql)
	datas, columns, err := e.processSrcDatas(sql)
	if err != nil {
		r := &TaskResult{
			taskName:  e.taskName,
			wid:       wid,
			start:     tp.start,
			end:       tp.end,
			insertNum: int64(0),
			err:       err,
		}
		return r
	}
	insertNum, err := e.writer.Write("insert", e.taskInfo.ToDb, e.taskInfo.ToTable, datas, columns, e.writeBatch)
	r := &TaskResult{
		taskName:  e.taskName,
		wid:       wid,
		start:     tp.start,
		end:       tp.end,
		insertNum: insertNum,
		err:       err,
	}
	logger.Infof("taskName:%s,wid:%d executeInit((%d,%d])", e.taskName, wid, tp.start, tp.end)
	return r
}

func (e *Executor) workerInit(wid int, tasksChan chan *TaskParams, resultsChan chan *TaskResult, dones chan int) {
	defer func() {
		dones <- wid
	}()
	for tp := range tasksChan {
		r := e.executeInit(wid, tp)
		resultsChan <- r
	}
}

func (e *Executor) getIncreaseActionData(tp *TaskParams) (map[string][]string, error) {
	srcQuery := fmt.Sprintf(baseQueryIncrease, e.srcPk, e.srcCheckKey, e.taskInfo.FromDb, e.taskInfo.FromTable, e.srcPk, tp.start, e.srcPk, tp.end)
	logger.Infof("taskName:%s,getIncreaseActionData srcQuerysql:%s", e.taskName, srcQuery)
	srcDatas, _, err := e.reader.Query(srcQuery)
	if err != nil {
		return nil, err
	}
	destQuery := fmt.Sprintf(baseQueryIncrease, e.destPk, e.destCheckKey, e.taskInfo.ToDb, e.taskInfo.ToTable, e.destPk, tp.start, e.destPk, tp.end)
	logger.Infof("taskName:%s,getIncreaseActionData destQuerysql:%s", e.taskName, destQuery)
	destDatas, _, err := e.writer.Query(destQuery)
	if err != nil {
		return nil, err
	}
	actionDatas := make(map[string][]string)
	srcFull := make(map[string]map[string]string)
	destUpdates := make([]string, 0)
	destInserts := make([]string, 0)
	destDeletes := make([]string, 0)
	for len(srcDatas) > 0 {
		src := srcDatas[0]
		srcDatas = append(srcDatas[:0], srcDatas[1:]...)
		srcFull[src["id"]] = src
	}
	for _, dest := range destDatas {
		//都存在时候 判断是否更新
		if src, ok := srcFull[dest["id"]]; ok {
			if src["updated"] != dest["updated"] {
				destUpdates = append(destUpdates, dest["id"])
				delete(srcFull, dest["id"])
			} else {
				delete(srcFull, dest["id"])
			}
		} else { //dest有src不存在 放入删除列表
			destDeletes = append(destDeletes, dest["id"])
		}
	}
	//destData = nil
	for k := range srcFull {
		destInserts = append(destInserts, k)
	}
	//srcFull = nil
	actionDatas["insert"] = destInserts
	actionDatas["update"] = destUpdates
	actionDatas["delete"] = destDeletes
	return actionDatas, nil

}

//增量对比
func (e *Executor) executeIncrease(wid int, tp *TaskParams) *TaskResult {
	actionDatas, err := e.getIncreaseActionData(tp)
	if err != nil {
		logger.Infof("taskName:%s,wid:%d getIncreaseActionData((%d,%d]) error:%+v", e.taskName, wid, tp.start, tp.end, err)
		r := &TaskResult{
			taskName: e.taskName,
			wid:      wid,
			start:    tp.start,
			end:      tp.end,
			err:      err,
		}
		return r
	}

	insertNum := int64(len(actionDatas["insert"]))
	updateNum := int64(len(actionDatas["update"]))
	deleteNum := int64(len(actionDatas["delete"]))
	affectNum := int64(0)
	deleteWheres := make([]string, 0)
	deleteWheres = append(deleteWheres, actionDatas["delete"]...)
	actionDatas["delete"] = nil
	deleteWheres = append(deleteWheres, actionDatas["update"]...)
	delteTemp := make([]string, 0)
	for len(deleteWheres) > 0 {
		delId := deleteWheres[0]
		deleteWheres = append(deleteWheres[:0], deleteWheres[1:]...)
		delteTemp = append(delteTemp, delId)
		if len(delteTemp) == e.writeBatch {
			delSql := e.baseDeleteDestSql
			deleteIdStrs := strings.Join(delteTemp, ",")
			deleteSql := fmt.Sprintf(strings.ReplaceAll(delSql, "$where", deleteIdStrs))
			//目标库删除
			lock.Lock()
			affectDelete, err := e.writer.Execute(deleteSql)
			lock.Unlock()
			if err != nil {
				logger.Errorf("taskName:%d deletesql is %s,\n error:%+v", e.taskName, deleteSql, err)
				r := &TaskResult{
					taskName: e.taskName,
					wid:      wid,
					start:    tp.start,
					end:      tp.end,
					err:      err,
				}
				return r
			}
			affectNum += affectDelete
			delteTemp = make([]string, 0)
		}
	}
	if len(delteTemp) > 0 {
		delSql := e.baseDeleteDestSql
		deleteIdStrs := strings.Join(delteTemp, ",")
		deleteSql := fmt.Sprintf(strings.ReplaceAll(delSql, "$where", deleteIdStrs))
		lock.Lock()
		affectDelete, err := e.writer.Execute(deleteSql)
		lock.Unlock()
		if err != nil {
			logger.Errorf("taskName:%d deletesql is %s,\n error:%+v", e.taskName, deleteSql, err)
			r := &TaskResult{
				taskName: e.taskName,
				wid:      wid,
				start:    tp.start,
				end:      tp.end,
				err:      err,
			}
			return r
		}
		affectNum += affectDelete
		delteTemp = nil
	}
	//处理批量插入或者批量更新
	replaces := make([]string, 0)
	replaces = append(replaces, actionDatas["update"]...)
	replaces = append(replaces, actionDatas["insert"]...)
	actionDatas = nil
	temp := make([]string, 0)
	for len(replaces) > 0 {
		replceId := replaces[0]
		temp = append(temp, replceId)
		replaces = append(replaces[:0], replaces[1:]...)
		if len(temp) == e.writeBatch {
			insertQuery := e.baseQuerySrcByPkSql
			insertIdStrs := strings.Join(temp, ",")
			insertQuery = strings.ReplaceAll(insertQuery, "$where", insertIdStrs)
			datas, columns, err := e.processSrcDatas(insertQuery)
			if err != nil {
				logger.Errorf("taskName:%d executeIncrease query src error:%+v", e.taskName, err)
				r := &TaskResult{
					taskName: e.taskName,
					wid:      wid,
					start:    tp.start,
					end:      tp.end,
					err:      err,
				}
				return r

			}
			insertNum, err := e.writer.Write("replace", e.taskInfo.ToDb, e.taskInfo.ToTable, datas, columns, e.writeBatch)
			if err != nil {
				logger.Errorf("taskName:%d executeIncrease.writer write error:%+v", e.taskName, err)
				r := &TaskResult{
					taskName: e.taskName,
					wid:      wid,
					start:    tp.start,
					end:      tp.end,
					err:      err,
				}
				return r
			}
			affectNum += insertNum
			temp = make([]string, 0)
		}
	}
	if len(temp) > 0 {
		insertQuery := e.baseQuerySrcByPkSql
		insertIdStrs := strings.Join(temp, ",")
		insertQuery = strings.ReplaceAll(insertQuery, "$where", insertIdStrs)
		datas, columns, err := e.processSrcDatas(insertQuery)
		if err != nil {
			logger.Errorf("taskName:%d executeIncrease query src error:%+v", e.taskName, err)
			r := &TaskResult{
				taskName: e.taskName,
				wid:      wid,
				start:    tp.start,
				end:      tp.end,
				err:      err,
			}
			return r
		}
		insertNum, err := e.writer.Write("replace", e.taskInfo.ToDb, e.taskInfo.ToTable, datas, columns, e.writeBatch)
		if err != nil {
			logger.Errorf("taskName:%d executeIncrease.writer write error:%+v", e.taskName, err)
			r := &TaskResult{
				taskName: e.taskName,
				wid:      wid,
				start:    tp.start,
				end:      tp.end,
				err:      err,
			}
			return r
		}
		affectNum += insertNum
		temp = nil
	}

	r := &TaskResult{
		taskName:  e.taskName,
		wid:       wid,
		start:     tp.start,
		end:       tp.end,
		insertNum: insertNum,
		updateNum: updateNum,
		deleteNum: deleteNum,
		affectNum: affectNum,
		err:       nil,
	}
	logger.Infof("taskName:%s,wid:%d executeIncrease((%d,%d])", e.taskName, wid, tp.start, tp.end)
	return r
}

func (e *Executor) workerIncrease(wid int, tasksChan chan *TaskParams, resultsChan chan *TaskResult, dones chan int) {
	defer func() {
		dones <- wid
	}()
	for tp := range tasksChan {
		r := e.executeIncrease(wid, tp)
		resultsChan <- r
	}
}

func (e *Executor) Run() *Result {
	defer func() {
		e.Close()
	}()
	//parseParmas valid json
	var err error
	err = e.parseParams()
	if err != nil {
		e.taskStatus = PARSEPARAMSERROR
		result := &Result{
			taskName:   e.taskName,
			taskStatus: e.taskStatus,
			err:        err,
		}
		return result
	}
	//get minid maxId
	var minId int64
	var maxId int64
	var srcEmpty bool
	if e.syncer.mode == "init" {
		minId, maxId, err, srcEmpty = e.getMinMaxInit()
	} else {
		minId, maxId, err, srcEmpty = e.getMinMaxIncrease()
	}
	if err != nil {
		e.taskStatus = GETMINMAXERROR
		result := &Result{
			taskName:   e.taskName,
			taskStatus: e.taskStatus,
			err:        err,
		}
		return result
	}
	tasksChan := e.produceTaskParams(minId, maxId, srcEmpty)
	resultsChan := make(chan *TaskResult)
	dones := make(chan int, e.workerNum)
	for i := 0; i < e.workerNum; i++ {
		if e.syncer.mode == "init" {
			go e.workerInit(i, tasksChan, resultsChan, dones)
		} else {
			go e.workerIncrease(i, tasksChan, resultsChan, dones)
		}
	}
	go func(resultsChan chan *TaskResult, dones chan int) {
		for i := 0; i < e.workerNum; i++ {
			wid := <-dones
			logger.Infof("taskname:%s worker:%d finished", e.taskName, wid)
		}
		close(resultsChan)
		logger.Infof("taskname:%s closed resultsChan", e.taskName)
	}(resultsChan, dones)
	result := &Result{
		taskName:  e.taskName,
		insertNum: int64(0),
		updateNum: int64(0),
		deleteNum: int64(0),
		affectNum: int64(0),
	}
	for r := range resultsChan {
		if r.err != nil {
			result.err = err
			result.taskStatus = FAILED
			logger.Errorf("taskName:%s,error:%v", e.taskName, r.err)
			return result
		} else {
			result.insertNum += r.insertNum
			result.updateNum += r.updateNum
			result.deleteNum += r.deleteNum
			result.affectNum += r.affectNum
			logger.Infof("taskName:%s,wid:%d,start-end(%d,%d],insertNum:%d,updateNum:%d,deleteNum:%d,affectNum:%d", e.taskName, r.wid, r.start, r.end, r.insertNum, r.updateNum, r.deleteNum, r.affectNum)
		}
	}
	msg := fmt.Sprintf("taskName:%s finished,insertNum:%d,updateNum:%d,deleteNum:%d,affectNum:%d", e.taskName, result.insertNum, result.updateNum, result.deleteNum, result.affectNum)
	logger.Infof("%s", msg)
	result.err = nil
	result.taskStatus = SUCCESS
	e.syncer.robot.SendMsg(msg)
	return result
}
