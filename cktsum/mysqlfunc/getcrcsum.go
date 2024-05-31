// 获取crc32校验和

package mysqlfunc

import (
	"cktsum/common"
	"database/sql"
	"github.com/shopspring/decimal"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// 定义MySQL拆分列的区间值
type colIdx struct {
	beginVal string // 起始值
	endVal   string // 结束值
}

var IdxColName string = "" // 主键索引的第一列

// 并行计算crc32
func GetCrc32Sum() decimal.Decimal {
	idxChan := make(chan colIdx, common.Parallel+1)
	crc32Chan := make(chan decimal.Decimal, 100000)

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	go func() {
		defer wg1.Done()
		splitIdx(idxChan)
	}()

	for i := 0; i < common.Parallel; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			getCrc32(idxChan, crc32Chan)
		}()
	}

	wg1.Wait()
	close(idxChan)
	wg2.Wait()
	close(crc32Chan)

	totalCrc32Sum := decimal.NewFromFloat(0.0)
	for crc := range crc32Chan {
		totalCrc32Sum = totalCrc32Sum.Add(crc)
	}

	return totalCrc32Sum
}

// 获取crc32校验和
func getCrc32(idxChan chan colIdx, partcrc chan decimal.Decimal) {
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

	// 如果非并行模式
	if !PartMode {
		err := dbconn.QueryRow(commonSqlStr).Scan(&valueStr)
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

// 构造查询SQL语句
func genQuerySql() string {
	// 定义列名，列类型
	type colType struct {
		colName string
		colType string
	}

	var colTypeInfo []colType

	// 获取mysql的列类型
	colTypeSql := "select column_name, data_type from information_schema.columns where table_schema = '" + Table.Owner + "' and table_name = '" +
		Table.Name + "' and column_name in ('" + strings.Join(*Collist, "','") + "') order by column_name collate utf8_general_ci"

	rows, err := MysConn.Query(colTypeSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	var colN string
	var colT string
	for rows.Next() {
		if err := rows.Scan(&colN, &colT); err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}
		colTypeInfo = append(colTypeInfo, colType{colN, colT})
	}

	// 构造查询的SQL语句
	mCrc32Sql := "select sql_no_cache sum(0"

	// 初始化varchar
	mVcharS := []string{"date", "datetime", "time", "timestamp", "year", "varchar", "text", "tinytext", "mediumtext", "longtext", "enmu", "set",
		"json", "boolean"}
	mVarcharStr := ""
	for _, ct := range colTypeInfo {
		for _, char := range mVcharS {
			if char == ct.colType {
				mVarcharStr = mVarcharStr + "ifnull(`" + ct.colName + "`," + `""),`
				break
			}
		}
	}

	// 初始化char, 需要截断右边的空格
	mCharStr := ""
	for _, ct := range colTypeInfo {
		if ct.colType == "char" {
			mCharStr = mCharStr + "ifnull(`" + ct.colName + "`," + `""),`
		}
	}

	// 初始化二进制
	mBlobStr := ""
	mBlobS := []string{"binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob"}
	for _, ct := range colTypeInfo {
		for _, blob := range mBlobS {
			if blob == ct.colType {
				mBlobStr = mBlobStr + "ifnull(crc32(`" + ct.colName + "`),0) + "
				break
			}
		}
	}

	// 初始化数字
	mNumS := []string{"bigint", "smallint", "tinyint", "int", "mediumint", "decimal", "float", "double"}
	mNumStr := ""
	for _, ct := range colTypeInfo {
		for _, num := range mNumS {
			if num == ct.colType {
				mNumStr = mNumStr + "ifnull(`" + ct.colName + "`,0) + "
				break
			}
		}
	}

	// 构造MySQL的SQL语句
	if mVarcharStr != "" {
		mCrc32Sql = mCrc32Sql + " + crc32(concat(" + mVarcharStr
		if mCharStr != "" {
			mCrc32Sql = mCrc32Sql + mCharStr[:len(mCharStr)-1] + "))"
		} else {
			mCrc32Sql = mCrc32Sql[:len(mCrc32Sql)-1] + "))"
		}
	} else {
		if mCharStr != "" {
			mCrc32Sql = mCrc32Sql + " + concat(" + mCharStr[:len(mCharStr)-1] + ")"
		}
	}

	if mBlobStr != "" {
		mCrc32Sql = mCrc32Sql + " + " + mBlobStr[:len(mBlobStr)-2]
	}

	if mNumStr != "" {
		mCrc32Sql = mCrc32Sql + " + " + mNumStr[:len(mNumStr)-2]
	}

	return mCrc32Sql + ") from " + Table.Owner + "." + Table.Name + " " + Wherec
}

// 获取主键索引的第一列
func getFirstPri() {
	getIdxColSql := "select column_name from INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = '" + Table.Owner + "' and TABLE_NAME = '" +
		Table.Name + "' and key_name = 'PRIMARY' and seq_in_index = 1"

	rows, err := MysConn.Query(getIdxColSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	for rows.Next() {
		if err := rows.Scan(&IdxColName); err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%S) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}
	}

	return
}

// 判断是否要进行拆分，按照主键索引的第一个列拆分
func splitIdx(idxChan chan colIdx) {
	// 从统计信息和全局变量中获取大的值作为拆分的标准
	var tableRows uint
	getTabRowSql := "select table_rows from information_schema.tables where table_schema = '" + Table.Owner + "' and table_name = '" +
		Table.Name + "'"
	err := MysConn.QueryRow(getTabRowSql).Scan(&tableRows)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	if tableRows < common.RowCount {
		tableRows = common.RowCount
	}

	// 小于100W不拆分, 并行等于1不拆分, 传入空值
	if tableRows < 100*10000 || common.Parallel == 1 {
		PartMode = false
		return
	}

	// 获取更新主键索引的第一列
	getFirstPri()

	firstQuery := "select " + IdxColName + " from " + Table.Owner + "." + Table.Name + " order by " +
		IdxColName + " limit " + strconv.Itoa(common.FetchSize) + ",1"
	nextQuery := "select " + IdxColName + " from " + Table.Owner + "." + Table.Name + " where " +
		IdxColName + " > ?" + " order by " + IdxColName + " limit " + strconv.Itoa(common.FetchSize) + ",1"

	stmt, err := MysConn.Prepare(nextQuery)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer stmt.Close()

	skipnum := 0 // 计数器
	var (
		idxCurVal string
		idxPreVal string
	)

	for {
		if skipnum == 0 {
			err := stmt.QueryRow(firstQuery).Scan(&idxCurVal)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
			// 放入第一行索引数据
			idxChan <- colIdx{beginVal: "", endVal: idxCurVal}
			idxPreVal = idxCurVal
		} else {
			err := stmt.QueryRow(nextQuery).Scan(&idxCurVal)
			if err != nil {
				if err == sql.ErrNoRows {
					idxChan <- colIdx{beginVal: idxPreVal, endVal: ""} // 最后一行索引数据
					return
				} else {
					_, file, line, _ := runtime.Caller(0)
					log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
				}
			}
			// 放入后面的索引数据
			idxChan <- colIdx{beginVal: idxPreVal, endVal: idxCurVal}
			idxPreVal = idxCurVal
		}
		skipnum++
	}
	return
}
