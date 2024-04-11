package main

import (
	"fmt"
	"flag"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"github.com/chengcxy/go_mysql2mysql/internal/logger"
	"github.com/chengcxy/go_mysql2mysql/internal/utils"
)

var ConfigPath string
var Env string
var config *configor.Config
var err error

func init() {
	flag.StringVar(&ConfigPath, "c", "../config/", "配置文件目录")
	flag.StringVar(&Env, "e", "dev", "运行的环境-json文件前缀 dev/test/prod")
	flag.Parse()
	config,err = configor.NewConfig(ConfigPath, Env, UsedEnv)
	if err != nil{
		fmt.Println("初始化解析配置文件错误...",err)
		panic(err)
	}
	logConf, _ := config.Get("log")
	logPath := logConf.(map[string]interface{})["log_path"].(string)
	logger.InitLogger(logger.Config{
		Dev:           true,
		NeedFileWrite: true,
		LogPath:       logPath,
		FilePrefix:    "",
	})
	extendConfig := 
	logger.InitLogger(logger.Config{
		Dev:           true,
		NeedFileWrite: true,
		LogPath:       "./logs",
		FilePrefix:    "data",
	})
	logger.Infof("configEnv %s", config.Env)
}

func main() {
	
}
