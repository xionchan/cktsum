package mysqlfunc

import (
	"cktsum/common"
	"database/sql"
	"github.com/shopspring/decimal"
	"log"
	"regexp"
	"runtime"
	"strings"
)

// 通过分区并行计算crc32
func paraPartCrc(partNameChan chan string, partcrc chan decimal.Decimal) {
	// 每个进程都要创建一个数据库连接来并行计算
	dbconn, err := common.CreateDbConn(Dsn)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer dbconn.Close()

	commonSqlStr := genQuerySql()

	var valueStr sql.NullString
	numsum := decimal.NewFromFloat(0.0)

	var queryStr string

	if Wherec == "" {
		queryStr = commonSqlStr + " partition({partname})"
	} else {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta("where") + `\b`)
		queryStr = re.ReplaceAllString(commonSqlStr, " partition({partname}) where")
	}

	for partName := range partNameChan {
		querySql := strings.Replace(queryStr, "{partname}", partName, -1)
		err := dbconn.QueryRow(querySql).Scan(&valueStr)
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}

		if valueStr.Valid {
			tempSum, err := decimal.NewFromString(valueStr.String)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
			numsum = numsum.Add(tempSum)
		}
	}

	partcrc <- numsum
	return
}
