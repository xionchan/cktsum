// 数据获取到外部计算

package oraclefunc

import (
	"cktsum/common"
	"database/sql"
	"fmt"
	"github.com/godror/godror"
	"log"
	"runtime"
	"strconv"
)

// 将数据传输到客户端逐行计算crc32
func extGetCrc32(rowidr chan [2]string, partcrc chan float64) {
	// 每个进程都要创建一个数据库连接来并行计算
	dbconn, err := common.CreateDbConn(Dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer dbconn.Close()

	oCrcSql := genQuerySql()

	// 定义存放一行数据的接口切片的长度, 通过查询SQL获取
	var rowidDataSql, checkColLenSql string
	var rows *sql.Rows
	if Wherec == "" || PartMode {
		checkColLenSql = oCrcSql + " where rownum < 2"
		rowidDataSql = oCrcSql + " where rowid between :1 and :2"
	} else {
		checkColLenSql = oCrcSql + " and rownum < 2"
		rowidDataSql = oCrcSql + " and rowid between :1 and :2"
	}

	rows, err = dbconn.Query(checkColLenSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	// 定义存储数据的接口切片
	dataInterCount := len(columns)
	rowStr := make([]interface{}, dataInterCount)
	for i := 0; i < dataInterCount; i++ {
		rowStr[i] = new(interface{})
	}

	stmt, err := dbconn.Prepare(rowidDataSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer stmt.Close()

	// 格式化时间格式
	_, err = dbconn.Exec("alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	_, err = dbconn.Exec("alter session set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss.FF'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	_, err = dbconn.Exec("alter session set nls_timestamp_tz_format = 'yyyy-mm-dd hh24:mi:ss.FF'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	var crc32 float64 = 0
	var onecrc uint64 = 0

	for rowid := range rowidr {
		if rowid[0] == "" {
			rows, err = dbconn.Query(oCrcSql)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		} else {
			rows, err = stmt.Query(rowid[0], rowid[1])
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		}

		for rows.Next() {
			err := rows.Scan(rowStr...)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
			for i := 0; i < dataInterCount; i++ {
				val := *rowStr[i].(*interface{})
				switch val.(type) {
				case string:
					by := []byte(val.(string))
					cr32 := common.ComputeCrc(by)
					onecrc += cr32
				case godror.Number:
					intval := fmt.Sprintf("%v", val)
					num, err := strconv.ParseFloat(intval, 32)
					if err != nil {
						_, file, line, _ := runtime.Caller(0)
						log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
					}
					crc32 += num
				default:
					cr32 := common.ComputeCrc(val.([]byte))
					onecrc += cr32
				}
			}
		}
	}
	partcrc <- crc32 + float64(onecrc)
	return
}
