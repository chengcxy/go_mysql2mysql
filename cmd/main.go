package main

import (
	"flag"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/syncer"
	"github.com/chengcxy/go_mysql2mysql/internal/utils"
)

var ConfigPath string
var Env string
var config *configor.Config
var Condition string
var err error

func init() {
	flag.StringVar(&ConfigPath, "c", "../config/", "配置文件目录")
	flag.StringVar(&Env, "e", "dev", "运行的环境-json文件前缀 dev/test/prod")
	flag.StringVar(&Condition, "condition", " 1=1 ", "条件")
	flag.Parse()
	config, err = configor.NewConfig(ConfigPath, Env, UsedEnv)
	if err != nil {
		fmt.Println("初始化解析配置文件错误...", err)
		panic(err)
	}
	logJsonConf, ok := config.Get("log")
	if !ok {
		fmt.Println("配置文件log配置不存在...")
		panic("配置文件log配置不存在...")
	}
	logConf := logJsonConf.(map[string]interface{})
	logConfig := logger.Config{
		Dev:           logConf["dev"].(bool),
		NeedFileWrite: logConf["need_file_write"].(bool),
		LogPath:       logConf["log_path"].(string),
		FilePrefix:    "go_mysql2mysql_",
	}
	logger.InitLogger(logConfig)
	logger.Infof("configEnv %s", config.Env)
}

func main() {
	s, err := syncer.NewSyncer(config, Condition)
	if err != nil {
		logger.Errorf("syncer.NewSyncer 初始化失败 %s", err)
		panic(err)
	}
	s.Run()

}
