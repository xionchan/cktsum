// 通过数据库内部计算

package oraclefunc

import (
	"cktsum/common"
	"database/sql"
	"github.com/shopspring/decimal"
	"log"
	"runtime"
)

// 在数据库内部使用Java函数计算crc32
func dbGetCrc32(rowidr chan [2]string, partcrc chan decimal.Decimal) {
	// 每个进程都要创建一个数据库连接来并行计算
	dbconn, err := common.CreateDbConn(Dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer dbconn.Close()

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

	// 计算返回一行的个数
	var rowidDataSql string
	var valueStr sql.NullString
	numsum := decimal.NewFromFloat(0.0)
	oCrcSql := genQuerySql()

	// 非并行模式
	if !ParaMode {
		err := dbconn.QueryRow(oCrcSql).Scan(&valueStr)
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}

		if valueStr.Valid {
			numsum, err = decimal.NewFromString(valueStr.String)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
			partcrc <- numsum
		} else {
			partcrc <- decimal.NewFromFloat(0.0)
		}

		return
	}

	if Wherec == "" || PartMode {
		rowidDataSql = oCrcSql + " where rowid between :1 and :2"
	} else {
		rowidDataSql = oCrcSql + " and rowid between :1 and :2"
	}
	stmt, err := dbconn.Prepare(rowidDataSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())

	}
	defer stmt.Close()

	for rowid := range rowidr {
		rows, err := stmt.Query(rowid[0], rowid[1])
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}

		for rows.Next() {
			err = rows.Scan(&valueStr)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}

			if valueStr.Valid {
				numsum, err = decimal.NewFromString(valueStr.String)
				if err != nil {
					_, file, line, _ := runtime.Caller(0)
					log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
				}
				partcrc <- numsum
			} else {
				partcrc <- decimal.NewFromFloat(0.0)
			}
		}
	}
	return
}
