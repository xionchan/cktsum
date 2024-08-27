// 数据获取到外部计算

package oraclefunc

import (
	"cktsum/common"
	"database/sql"
	"fmt"
	"github.com/godror/godror"
	"github.com/shopspring/decimal"
	"log"
	"math/big"
	"runtime"
)

// external模式下，非并行模式下计算crc32
func extNoParaGetCrc32(partcrc chan decimal.Decimal) {
	commonSqlStr := genQuerySql()
	// 定义存放一行数据的接口切片的长度, 通过查询SQL获取
	var checkColLenSql string
	if Wherec == "" || PartMode {
		checkColLenSql = commonSqlStr + " where rownum < 2"
	} else {
		checkColLenSql = commonSqlStr + " and rownum < 2"
	}

	rows, err := OraConn.Query(checkColLenSql)
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

	// 格式化时间格式
	_, err = OraConn.Exec("alter session set nls_date_format = 'yyyy-mm-dd'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	_, err = OraConn.Exec("alter session set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss.FF'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	_, err = OraConn.Exec("alter session set nls_timestamp_tz_format = 'yyyy-mm-dd hh24:mi:ss.FF'")
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	// 定义存放数字和crc的变量
	numsum := decimal.NewFromFloat(0.0)
	var onecrc uint64 = 0

	rows, err = OraConn.Query(commonSqlStr)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
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
				num, err := decimal.NewFromString(intval)
				// num, err := strconv.ParseFloat(intval, 32)
				if err != nil {
					_, file, line, _ := runtime.Caller(0)
					log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
				}
				numsum = numsum.Add(num)
			default:
				cr32 := common.ComputeCrc(val.([]byte))
				onecrc += cr32
			}
		}
	}
	numsum = numsum.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(onecrc), 0))
	partcrc <- numsum
	return
}

// 将数据传输到客户端逐行计算crc32
func extParaGetCrc32(rowidr chan [2]string, partcrc chan decimal.Decimal) {
	// 每个进程都要创建一个数据库连接来并行计算
	dbconn, err := common.CreateDbConn(Dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer dbconn.Close()

	commonSqlStr := genQuerySql()

	// 定义存放一行数据的接口切片的长度, 通过查询SQL获取
	var rowidDataSql, checkColLenSql string
	var rows *sql.Rows
	if Wherec == "" || PartMode {
		checkColLenSql = commonSqlStr + " where rownum < 2"
		rowidDataSql = commonSqlStr + " where rowid between :1 and :2"
	} else {
		checkColLenSql = commonSqlStr + " and rownum < 2"
		rowidDataSql = commonSqlStr + " and rowid between :1 and :2"
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

	// var crc32 decimal.Decimal
	numsum := decimal.NewFromFloat(0.0)
	var onecrc uint64 = 0

	for rowid := range rowidr {
		rows, err = stmt.Query(rowid[0], rowid[1])
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
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
					num, err := decimal.NewFromString(intval)
					// num, err := strconv.ParseFloat(intval, 32)
					if err != nil {
						_, file, line, _ := runtime.Caller(0)
						log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
					}
					numsum = numsum.Add(num)
				default:
					cr32 := common.ComputeCrc(val.([]byte))
					onecrc += cr32
				}
			}
		}
	}
	numsum = numsum.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(onecrc), 0))
	partcrc <- numsum
	return
}
