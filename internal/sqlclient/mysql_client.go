package sqlclient

import (
	"database/sql"
	"errors"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
	"time"
)

//查询表的列字段
var QUERY_TABLE_COLUMNS = `
select column_name,column_type,column_comment,column_key
from information_schema.columns
where table_schema="%s" and table_name="%s"
`

//查询表的唯一索引
var QUERY_UNIQ_INDEXS = "show index from %s.%s where non_unique = 0"

//表的元数据信息 数据库名 表名 主键 最大值 最小值 切分的任务列表 所属的客户端

//查询元数据
func (m *MysqlClient) GetTableMeta(own_app, db_name, table_name string) (*TableMeta, error) {
	sql := fmt.Sprintf(QUERY_TABLE_COLUMNS, db_name, table_name)
	rows_list, _, err := m.Query(sql)
	if err != nil {
		return &TableMeta{}, err
	}
	fields := make([]string, len(rows_list))
	pk := ""
	hasPk := false
	for index, item := range rows_list {
		fields[index] = strings.ToLower(item["column_name"])
		if item["column_key"] == "PRI" {
			pk = strings.ToLower(item["column_name"])
			hasPk = true
		}
	}
	if !hasPk {
		return &TableMeta{}, errors.New("table no pk")
	}
	minId, err := m.GetMinId(db_name, table_name, pk)
	if err != nil {
		return &TableMeta{}, err
	}
	maxId, err := m.GetMaxId(db_name, table_name, pk)
	if err != nil {
		return &TableMeta{}, err
	}
	unique_indexs, err := m.GetUniqueIndexs(db_name, table_name)
	if err != nil {
		return &TableMeta{}, err
	}
	tm := &TableMeta{
		OwnApp:        own_app,
		DbName:        db_name,
		TableName:     table_name,
		Pk:            pk,
		Fields:        fields,
		UniqueIndexs:  unique_indexs,
		MinId:         minId,
		MaxId:         maxId,
		HasPrimaryKey: hasPk,
	}
	return tm, nil
}

func (m *MysqlClient) GetUniqueIndexs(db_name, table_name string) ([]string, error) {
	sql := fmt.Sprintf(QUERY_UNIQ_INDEXS, db_name, table_name)
	rows_list, _, err := m.Query(sql)
	if err != nil {
		return nil, errors.New("获取唯一索引失败")
	}
	unique_indexs := make([]string, len(rows_list))
	if len(rows_list) == 0 {
		return unique_indexs, nil
	}
	for index, item := range rows_list {
		unique_indexs[index] = strings.ToLower(item["column_name"])
	}

	return unique_indexs, nil
}

//关闭数据库连接池
func (m *MysqlClient) Close() {
	m.Db.Close()
}

//获取表最小值
func (m *MysqlClient) GetTotalCount(db_name, table_name string) (int, error) {
	query := fmt.Sprintf("select count(1) as total from %s.%s ", db_name, table_name)
	rows, _, err := m.Query(query)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("get table total count error:%v", err))
	}
	data := rows[0]["total"]
	total, err := strconv.Atoi(data)
	if err != nil {
		return 0, err
	}
	return total, nil
}

//获取表最小值
func (m *MysqlClient) GetMinId(db_name, table_name, pk string) (int, error) {
	query := fmt.Sprintf("select %s from %s.%s order by %s limit 1", pk, db_name, table_name, pk)
	rows, _, err := m.Query(query)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("get table minId  error:%v", err))
	}
	if len(rows) == 0 {
		return 0, errors.New("not get table minId ")
	}
	minIdStr := rows[0][pk]
	minId, err := strconv.Atoi(minIdStr)
	if err != nil {
		return 0, err
	}
	return minId - 1, nil
}

//获取表最大值
func (m *MysqlClient) GetMaxId(db_name, table_name, pk string) (int, error) {
	query := fmt.Sprintf("select %s from %s.%s order by %s desc limit 1", pk, db_name, table_name, pk)
	rows, _, err := m.Query(query)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("get table maxId  error:%v", err))
	}

	if len(rows) == 0 {
		return 0, errors.New("not get table maxId ")
	}
	maxIdStr := rows[0][pk]
	maxId, err := strconv.Atoi(maxIdStr)
	if err != nil {
		return 0, err
	}
	return maxId, nil
}

//mysql客户端结构体
type MysqlClient struct {
	Db *sql.DB
}

//mysql 客户端
func NewMysqlClient(config *configor.Config, key string) (*MysqlClient, error) {
	c := &MysqlClient{}
	Db, err := c.Connect(config, key)
	client := &MysqlClient{
		Db: Db,
	}
	return client, err
}

func (c *MysqlClient) Connect(config *configor.Config, key string) (*sql.DB, error) {
	conf, ok := config.Get(key)
	if !ok {
		return nil, errors.New(fmt.Sprintf("key:%s not in json_file", key))
	}
	fmt.Println("conf is ", conf)
	m := conf.(map[string]interface{})
	Uri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		m["user"].(string),
		m["password"].(string),
		m["host"].(string),
		int(m["port"].(float64)),
		m["db"].(string),
		m["charset"].(string),
	)
	Db, err := sql.Open("mysql", Uri)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("open mysql error:%v", err))
	}
	Db.SetConnMaxLifetime(time.Minute * 100)
	MaxOpenConns, ok := m["MaxOpenConns"]
	if ok {
		Db.SetMaxOpenConns(int(MaxOpenConns.(float64)))
	} else {
		Db.SetMaxOpenConns(20)
	}
	MaxIdleConns, ok := m["MaxIdleConns"]
	if ok {
		Db.SetMaxIdleConns(int(MaxIdleConns.(float64)))
	} else {
		Db.SetMaxIdleConns(20)
	}
	err = Db.Ping()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ping mysql error:%v", err))
	}
	return Db, nil
}

//封装query方法
func (m *MysqlClient) Query(query string, args ...interface{}) ([]map[string]string, []string, error) {
	rows, err := m.Db.Query(query, args...)
	if err != nil {
		logger.Errorf("query stmt:%s error:%v", query, err)
		return nil, nil, err
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	for index, col := range columns {
		columns[index] = strings.ToLower(col)
	}
	scanArgs := make([]interface{}, len(columns))
	values := make([]sql.RawBytes, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	results := make([]map[string]string, 0)
	for rows.Next() {
		//将行数据保存到record字典
		err = rows.Scan(scanArgs...)
		record := make(map[string]string)
		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			record[strings.ToLower(columns[i])] = value
		}

		results = append(results, record)
	}

	return results, columns, nil
}

func (m *MysqlClient) Execute(stmt string, args ...interface{}) (int64, error) {
	var defaultNum int64
	var err error
	var result sql.Result
	for i := 0; i < 4; i++ {
		result, err = m.Db.Exec(stmt, args...)
		if err == nil {
			var affNum int64
			affNum, err = result.RowsAffected()
			if err == nil {
				return affNum, err
			}
		}
		logger.Errorf("第%d次 exec sql err:%v,begin next retry", i+1, err)

	}
	return defaultNum, errors.New(fmt.Sprintf("retry 4 times all error:%+v", err))
}

func (m *MysqlClient) Write(write_mode, to_db, to_table string, datas []map[string]string, columns []string, writeBatch int) (int64, error) {
	var num int64
	fields := make([]string, len(columns))
	for index, col := range columns {
		fields[index] = fmt.Sprintf("`%s`", col)
	}
	if len(datas) == 0 {
		return 0, nil
	} else {
		fieldStr := strings.Join(fields, ",")
		insertSql := fmt.Sprintf("%s into %s.%s(%s)values", write_mode, to_db, to_table, fieldStr)
		questionSigns := make([]string, len(columns))
		for i, _ := range columns {
			questionSigns[i] = "?"
		}
		questionSignStr := fmt.Sprintf("(%s)", strings.Join(questionSigns, ","))
		tempBatchs := make([]map[string]string, 0)
		for len(datas) > 0 {
			data := datas[0]
			tempBatchs = append(tempBatchs, data)
			datas = append(datas[:0], datas[1:]...)
			if len(tempBatchs) == writeBatch {
				batchCommitSql := insertSql
				qs := make([]string, writeBatch)
				values := make([]interface{}, len(columns)*len(tempBatchs))
				for index, data := range tempBatchs {
					v := make([]interface{}, len(columns))
					for j := 0; j < len(columns); j++ {
						if data[columns[j]] == "NULL" {
							v[j] = nil
						} else {
							v[j] = data[columns[j]]
						}
					}
					for x := 0; x < len(columns); x++ {
						values[index*len(columns)+x] = v[x]
					}
					qs[index] = questionSignStr
				}
				batchCommitSql += strings.Join(qs, ",")
				batchCommitNum, err := m.Execute(batchCommitSql, values...)
				if err != nil {
					return 0, err
				}
				num += batchCommitNum
				values = nil
				tempBatchs = make([]map[string]string, 0)
				batchCommitSql = ""
			}
		}
		if len(tempBatchs) > 0 {
			batchCommitSql := insertSql
			qs := make([]string, len(tempBatchs))
			values := make([]interface{}, len(columns)*len(tempBatchs))
			for index, data := range tempBatchs {
				v := make([]interface{}, len(columns))
				for j := 0; j < len(columns); j++ {
					if data[columns[j]] == "NULL" {
						v[j] = nil
					} else {
						v[j] = data[columns[j]]
					}
				}
				for x := 0; x < len(columns); x++ {
					values[index*len(columns)+x] = v[x]
				}
				qs[index] = questionSignStr
			}
			batchCommitSql += strings.Join(qs, ",")
			batchCommitNum, err := m.Execute(batchCommitSql, values...)
			if err != nil {
				return 0, err
			}
			num += batchCommitNum
			values = nil
			tempBatchs = nil
		}
		return num, nil
	}
}
