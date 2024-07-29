// 定义全局变量
package common

import "time"

var (
	SDSN      DBConnection     // 源库的连接串
	TDSN      DBConnection     // 目标库的连接串
	ST        Table            // 源库的表名
	TT        Table            // 目标库的表名
	Parallel  int              // 数据库并行度
	FetchSize int              // 每个分片的大小
	OraMode   string           // oracle的校验模式
	ColStr    string           // 传入的列信息
	SColList  []string         // 列列表
	TColList  []string         // 列列表
	TWhere    string           // 目标库where条件
	SWhere    string           // 源库where条件
	RowCount  uint         = 0 // 行数
	SPartMode bool             // 源库是否是分区模式
	TPartMode bool             // 目标库是否是分区模式
	StartTime time.Time        // 定义程序的开始时间
	CMode     string           // 定义数据校验的模式
)

// 定义数据库连接的结构
type DBConnection struct {
	Type     string // 数据库类型 ： oracle, mysql, postgresql
	User     string // 用户
	Password string // 密码
	Host     string // IP地址
	Port     string // 端口
	Database string // 服务名
}

// 定义表名的结构
type Table struct {
	Owner string // db或schema
	Name  string // 表名
}
