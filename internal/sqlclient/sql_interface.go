package sqlclient

import (
	"database/sql"
	"errors"
	"github.com/chengcxy/go_mysql2mysql/configor"
)

type TableMeta struct {
	OwnApp        string   `json:"own_app"`
	DbName        string   `json:"db_name"`
	TableName     string   `json:"table_name"`
	Pk            string   `json:"pk"`
	Fields        []string `json:"fields"`
	UniqueIndexs  []string `json:"unique_indexs"`
	MinId         int      `json:"min_id"`
	MaxId         int      `json:"max_id"`
	Batch         int      `json:"batch"`
	HasPrimaryKey bool     `json:"has_primary_key"`
	TotalCount    int      `json:"total_count"`
}

type SqlClient interface {
	Connect(config *configor.Config, key string) (*sql.DB, error)
	Close()
	GetTableMeta(own_app, db_name, table_name string) (*TableMeta, error)
	GetUniqueIndexs(db_name, table_name string) ([]string, error)
	GetTotalCount(db_name, table_name string) (int, error)
	GetMinId(db_name, table_name, pk string) (int, error)
	GetMaxId(db_name, table_name, pk string) (int, error)
	Query(query string, args ...interface{}) ([]map[string]string, []string, error)
	Execute(stmt string, args ...interface{}) (int64, error)
	Write(write_mode, to_db, to_table string, datas []map[string]string, columns []string, writeBatch int) (int64, bool, error)
}

func GetSqlClient(clientType string, config *configor.Config, key string) (SqlClient, error) {
	switch clientType {
	case "mysql":
		return NewMysqlClient(config, key)
	default:
		return nil, errors.New("not support client type")
	}
}
