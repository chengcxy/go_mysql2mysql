package sqlclient

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	_ "github.com/trinodb/trino-go-client/trino"
)


var QUERY_HIVE_TABLE_COLUMNS = `
SELECT 
    T.NAME as db_name,
    T1.TBL_NAME as table_name,
    T3.COLUMN_NAME as column_name
FROM hive.DBS as T
join hive.TBLS as T1 on T.DB_ID=T1.DB_ID  and T.OWNER_NAME =T1.OWNER
join hive.SDS as T2 on  T1.SD_ID=T2.SD_ID
join hive.COLUMNS_V2 as T3 on T2.CD_ID=T3.CD_ID
WHERE (T.NAME=? and T1.TBL_NAME=?)
ORDER BY T.NAME,T1.TBL_NAME,T3.INTEGER_IDX
`
type TrinoClient struct {
	MysqlClient
	metaClient SqlClient
}

func NewTrinoClient(config *configor.Config, key string) (*TrinoClient, error) {
	conf, ok := config.Get(key)
	if !ok {
		return nil, errors.New(fmt.Sprintf("key:%s not in json_file", key))
	}
	m := conf.(map[string]interface{})
	//http://用户名@host:port?catalog=hive&schema=dw
	dsn := fmt.Sprintf("http://%s@(%s:%d)?catalog=%s&schema=%s",
		m["user"].(string),
		m["host"].(string),
		int(m["port"].(float64)),
		m["catalog"].(string),
		m["schema"].(string),
	)
	db, err := sql.Open("trino", dsn)
	metaClient,_ := NewMysqlClient(config,"mysql.hive_meta")
	return &TrinoClient{
		MysqlClient: MysqlClient{Db: db},
		metaClient:metaClient,
	}, err
}



//表的元数据信息 数据库名 表名 字段列表
func (tc *TrinoClient) GetTableMeta(own_app, db_name, table_name string) (*TableMeta, error) {
	rows_list, _, err := tc.metaClient.Query(QUERY_HIVE_TABLE_COLUMNS,db_name,table_name)
	if err != nil {
		return &TableMeta{}, err
	}
	fields := make([]string, len(rows_list))
	for index, item := range rows_list {
		fields[index] = strings.ToLower(item["column_name"])

	}
	tm := &TableMeta{
		OwnApp:        own_app,
		DbName:        db_name,
		TableName:     table_name,
		Fields:        fields,
	}
	tc.metaClient.Close()
	return tm, nil
}

