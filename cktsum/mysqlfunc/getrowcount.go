// 获取表的行数

package mysqlfunc

import (
	"log"
	"runtime"
)

// 获取对应表的行数
func GetCount() uint {
	var rowcount uint

	err := MysConn.QueryRow("select sql_no_cache count(*) from " + Table.Owner + "." + Table.Name + " " + Wherec).Scan(&rowcount)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误 : 报错位置 %s:%d (%s) \n", file, line, err.Error())
	}

	return rowcount
}
