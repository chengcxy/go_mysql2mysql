package roboter

import (
	configor "github.com/chengcxy/go_mysql2mysql/config"
)

type Roboter interface{
	SendMsg(params ...string) (string,error)
	GetPayload(params ...string)([]byte,error)
}


func GetRoboter(config *configor.Config)(Roboter){
	c,_ := config.Get("roboter")
	robotType := c.(map[string]interface{})["roboter_type"].(string)
	if robotType == "weixin"{
		return NewWechatRoboter(config)
	}else{
		return NewDingTalkRoboter(config)
	} 
}


