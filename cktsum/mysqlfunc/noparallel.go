package mysqlfunc

import (
	"database/sql"
	"github.com/shopspring/decimal"
	"log"
	"runtime"
)

// 非并行模式计算
func noParaCrc(partcrc chan decimal.Decimal) {
	var valueStr sql.NullString
	numsum := decimal.NewFromFloat(0.0)
	commonSqlStr := genQuerySql()
	err := MysConn.QueryRow(commonSqlStr).Scan(&valueStr)
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
	}

	partcrc <- numsum
	return
}
