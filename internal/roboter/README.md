
# 报警机器人如何使用 

- 配置文件 比如dev.json 添加roboter的key 钉钉机器人配置规则

```json
{
    "roboter": {
        "token": "token",
        "atMobiles": [
            "$mobile"
        ],
        "isAtAll": false,
        "hook_keyword": "任务报警",
        "roboter_type": "dingding"
     }
}
```


```go
package main


import(
    "github.com/chengcxy/gotools/configor"
    "github.com/chengcxy/gotools/roboter"

)


func main(){
    ConfigPath := "/Users/chengxinyao/config"
	Env := "dev"
	//使用环境变量时 最后参数为true
	config := configor.NewConfig(ConfigPath,Env,true)
	//json.roboter.roboter_type=dingding 自动获取钉钉机器人
	robot := roboter.GetRoboter(config)
	robot.SendMsg("get robot")

}

```

