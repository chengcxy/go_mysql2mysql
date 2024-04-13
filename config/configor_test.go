package configor

import (
	"fmt"
	"testing"
)

var Usage = `
cd $project_path/configor
go test
`

func TestNewConfig(t *testing.T) {
	ConfigPath := "/data/go/src/github.com/chengcxy/config/"
	Env := "test"
	UsedEnv := true
	config := NewConfig(ConfigPath, Env, UsedEnv)
	fmt.Println(config.Conf)
	fmt.Println(config.Get("taskmeta.conn"))
	fmt.Println(config.Get("taskmeta.query_task"))
	c, _ := config.Get("job_meta_conf")
	fmt.Println(c)
	c, _ = config.Get("roboter")
	fmt.Println(c)
}
