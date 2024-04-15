package syncer

type TaskInfo struct {
	Id           string `json:"id"`            // 任务id
	FromApp      string `json:"from_app"`      // 读取的业务系统
	FromDbType   string `json:"from_db_type"`  // 读取的数据源类型mysql/oracle等
	FromDb       string `json:"from_db"`       // 读取的数据库
	FromTable    string `json:"from_table"`    // 读取的表
	ToApp        string `json:"to_app"`        // 写入的业务系统
	ToDbType     string `json:"to_db_type"`    // 写入数据源类型
	ToDb         string `json:"to_db"`         // 写入数据库
	ToTable      string `json:"to_table"`      // 写入数据表
	Params       string `json:"params"`        // 参数
	OnlineStatus string `json:"online_status"` // 在线状态0统计1不统计
	TaskDesc     string `json:"task_desc"`     // 统计描述
	Owner        string `json:"owner"`         // 开发所属人
	TaskStatus   string `json:"task_status"`   // 任务状态
}

type TaskParams struct {
	start int64
	end   int64
}

//
var baseQueryWaitedTasks = `
select *
from task_def_sync_manager
where online_status=1 and %s
`

var baseQueryMinMax = `
select min(%s)-1 as min_id,max(%s) as max_id
from %s.%s
`

var baseQuerySrcInit = `
select *
from %s.%s
where %s>%d and %s<=%d
`

var baseQueryIncrease = `
select %s as id,%s as updated
from %s.%s
where %s>%d and %s<=%d
`

var baseDeleteDestSql = `
delete from %s.%s
where %s in ($where)
`

var baseQuerySrcByPkSql = `
select *
from %s.%s
where %s in ($where)
`

//任务状态
var (
	PARSEPARAMSERROR = 1
	GETMINMAXERROR   = 2
	RUNNING          = 3
	FAILED           = 4
	SUCCESS          = 5
)
