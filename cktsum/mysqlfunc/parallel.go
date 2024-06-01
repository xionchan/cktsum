package mysqlfunc

import (
	"cktsum/common"
	"database/sql"
	"github.com/shopspring/decimal"
	"log"
	"runtime"
)

// 并行计算crc32
func paraCrc(idxChan chan colIdx, partcrc chan decimal.Decimal) {
	// 每个进程都要创建一个数据库连接来并行计算
	dbconn, err := common.CreateDbConn(Dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer dbconn.Close()

	commonSqlStr := genQuerySql()

	var (
		firstQuery string
		endQuery   string
		nextQuery  string
	)

	var valueStr sql.NullString
	numsum := decimal.NewFromFloat(0.0)

	if Wherec == "" || PartMode {
		firstQuery = commonSqlStr + " where " + IdxColName + " <= ?"
		endQuery = commonSqlStr + " where " + IdxColName + " > ?"
		nextQuery = commonSqlStr + " where " + IdxColName + " <= ? and " + IdxColName + " > ?"
	} else {
		firstQuery = commonSqlStr + " and " + IdxColName + " > ?"
		endQuery = commonSqlStr + " and " + IdxColName + " > ?"
		nextQuery = commonSqlStr + " and " + IdxColName + " <= ? and " + IdxColName + " > ?"
	}

	// only prepare中间的sql语句
	stmt, err := dbconn.Prepare(nextQuery)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer stmt.Close()

	for idxVal := range idxChan {
		if idxVal.beginVal == "" && idxVal.endVal != "" {
			err := dbconn.QueryRow(firstQuery, idxVal.endVal).Scan(&valueStr)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		} else if idxVal.endVal == "" && idxVal.beginVal != "" {
			err := dbconn.QueryRow(endQuery, idxVal.beginVal).Scan(&valueStr)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		} else if idxVal.beginVal != "" && idxVal.endVal != "" {
			err := stmt.QueryRow(idxVal.endVal, idxVal.beginVal).Scan(&valueStr)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		} else {
			err := dbconn.QueryRow(commonSqlStr).Scan(&valueStr)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		}

		if valueStr.Valid {
			numsum, err = decimal.NewFromString(valueStr.String)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
		}

		partcrc <- numsum
	}
	return
}
