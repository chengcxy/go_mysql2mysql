package roboter

import (
	"bytes"
	"encoding/json"
	"fmt"
	configor "github.com/chengcxy/go_mysql2mysql/config"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

/*
微信报警 json配置文件
"roboter": {
	"token": "token",
	"isAtAll": false,

}
*/

//钉钉机器人post请求接口地址
var WechatBaseApi = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="

type WechatRoboter struct {
	Token string
}

func (wr *WechatRoboter) resolveParams(params []string) string {
	switch len(params) {
	case 1:
		return params[0]
	default:
		panic(" WechatRoboter.parameters.length not equal 1")
	}
}

func (wr *WechatRoboter) SendMsg(params ...string) (string, error) {
	payload, err := wr.GetPayload(params...)
	if err != nil {
		log.Println("get wechat payload message error,", err)
		return "WechatRoboter.SendMsg.GetPayload  bytes error", err
	}
	contentType := "application/json"
	api := fmt.Sprintf(WechatBaseApi, wr.Token)
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	resp, err := client.Post(api, contentType, bytes.NewBuffer(payload))
	if err != nil {
		return "WechatRoboter.SendMsg.post error", err
	}
	defer resp.Body.Close()
	result, err := ioutil.ReadAll(resp.Body)
	return string(result), err
}

func (wr *WechatRoboter) GetPayload(params ...string) ([]byte, error) {
	content := wr.resolveParams(params)
	data := make(map[string]interface{})
	data["msgtype"] = "text"
	text := make(map[string]string)
	text["content"] = fmt.Sprintf("%s", content)
	data["text"] = text
	return json.Marshal(data)

}

func NewWechatRoboter(config *configor.Config) *WechatRoboter {
	v, _ := config.Get("roboter")
	rc := v.(map[string]interface{})
	token := rc["token"].(string)
	return &WechatRoboter{
		Token: token,
	}
}
