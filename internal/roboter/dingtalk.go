package roboter

import (
	"bytes"
	"encoding/json"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

/*
钉钉报警json配置文件  如果机器人设置了信息关键字 hook_keyword填写该关键字 否则无法推送成功
比如 我的机器人设置了"任务"2个字 只有消息里面含有任务字样才可以发送
"roboter": {
	"token": "token",
	"atMobiles": [
		"$mobile"
	],
	"isAtAll": false,
	"hook_keyword": "任务报警"
}

*/

//钉钉机器人post请求接口地址
var DingTalkBaseApi = "https://oapi.dingtalk.com/robot/send?access_token=%s"

type DingTalkRoboter struct {
	Token      string
	AtMobiles  []string
	Hookeyword string
	IsAtall    bool
}

func (dt *DingTalkRoboter) SendMsg(params ...string) (string, error) {
	payload, err := dt.GetPayload(params...)
	if err != nil {
		log.Println("get dingtalk payload message error,", err)
		return "DingTalkRoboter.SendMsg.GetPayload  bytes error", err
	}
	contentType := "application/json;charset=utf-8"
	api := fmt.Sprintf(DingTalkBaseApi, dt.Token)
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	resp, err := client.Post(api, contentType, bytes.NewBuffer(payload))
	if err != nil {
		return "send dingtalk msg err", err
	}
	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	return string(result), err

}

//params(context,mobile string)
func (dt *DingTalkRoboter) resolveParams(params []string) (string, []string) {
	switch len(params) {
	case 1:
		return params[0], dt.AtMobiles
	case 2:
		return params[0], strings.Split(params[1], ",")
	default:
		panic(" DingTalkRoboter.parameters.length not in (1,2)")
	}
}

func (dt *DingTalkRoboter) GetPayload(params ...string) ([]byte, error) {
	data := make(map[string]interface{})
	content, atMobiles := dt.resolveParams(params)
	data["msgtype"] = "text"
	at := make(map[string]interface{})
	at["atMobiles"] = atMobiles
	at["isAtAll"] = dt.IsAtall
	data["at"] = at
	text := make(map[string]string)
	text["content"] = fmt.Sprintf("%s:%s", dt.Hookeyword, content)
	data["text"] = text
	return json.Marshal(data)

}

func NewDingTalkRoboter(config *configor.Config) *DingTalkRoboter {
	v, _ := config.Get("roboter")
	rc := v.(map[string]interface{})
	token := rc["token"].(string)
	atMobiles := rc["atMobiles"].([]string)
	hook_keyword := rc["hook_keyword"].(string)
	isAtAll := rc["isAtAll"].(bool)
	return &DingTalkRoboter{
		Token:      token,
		AtMobiles:  atMobiles,
		Hookeyword: hook_keyword,
		IsAtall:    isAtAll,
	}
}
