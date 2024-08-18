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

var IdxColName string // 主键索引的第一列

// ii算crc32sum
func GetCrc32Sum() decimal.Decimal {
	crc32Chan := make(chan decimal.Decimal, 100000)

	if !ParaMode {
		noParaCrc(crc32Chan)
		close(crc32Chan)
	} else { // 并行模式, 先判断是通过索引并行,还是分区并行
		var wg1 sync.WaitGroup
		var wg2 sync.WaitGroup

		isPartTab := checkPartTable()
		if !isPartTab || PartMode {
			idxChan := make(chan colIdx, common.Parallel+1)
			// 更新索引第一列变量
			getFirstPri()

			wg1.Add(1)
			go func() {
				defer wg1.Done()
				splitIdx(idxChan)
			}()

			for i := 0; i < common.Parallel; i++ {
				wg2.Add(1)
				go func() {
					defer wg2.Done()
					paraCrc(idxChan, crc32Chan)
				}()
			}

			wg1.Wait()
			close(idxChan)
			wg2.Wait()
			close(crc32Chan)
		} else {
			partChan := make(chan string, common.Parallel+1)
			wg1.Add(1)
			go func() {
				defer wg1.Done()
				splitPart(partChan)
			}()

			for i := 0; i < common.Parallel; i++ {
				wg2.Add(1)
				go func() {
					defer wg2.Done()
					paraPartCrc(partChan, crc32Chan)
				}()
			}
			wg1.Wait()
			close(partChan)
			wg2.Wait()
			close(crc32Chan)
		}
	}

	totalCrc32Sum := decimal.NewFromFloat(0.0)
	for crc := range crc32Chan {
		totalCrc32Sum = totalCrc32Sum.Add(crc)
	}

	return totalCrc32Sum
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

// 判断是否是分区表
func checkPartTable() bool {
	isPartTab := true
	getPartTabSql := "select partition_name from INFORMATION_SCHEMA.PARTITIONS where TABLE_SCHEMA = '" + Table.Owner + "' and TABLE_NAME = '" +
		Table.Name + "' limit 1"

	var valueStr sql.NullString
	err := MysConn.QueryRow(getPartTabSql).Scan(&valueStr)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	if valueStr.Valid {
		isPartTab = true
	} else {
		isPartTab = false
	}

	return isPartTab
}

// 获取主键索引的第一列
func getFirstPri() {
	getIdxColSql := "select column_name from INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = '" + Table.Owner + "' and TABLE_NAME = '" +
		Table.Name + "' and index_name = 'PRIMARY' and seq_in_index = 1"

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

// 按照主键索引的第一个列拆分
func splitIdx(idxChan chan colIdx) {
	/*
		逻辑 ：
		第一个sql : select id from table limit order by id 10,1;  传入["", cur_id]
		后续的sql : select id from table where id > 11 order by id limit 10,1;   该语句中id大于的值是前一个sql获取的id值; 传入 [pre_id, cur_id]
		最后的sql : select id from table where id > 11 order by id limit 10,1;   获取的值为空值，传入[pre_id, ""]
	*/
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
			err := MysConn.QueryRow(firstQuery).Scan(&idxCurVal)
			if err != nil {
				_, file, line, _ := runtime.Caller(0)
				log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
			}
			// 放入第一行索引数据
			idxChan <- colIdx{beginVal: "", endVal: idxCurVal}
			idxPreVal = idxCurVal
		} else {
			err := stmt.QueryRow(idxPreVal).Scan(&idxCurVal)
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

// 按照分区进行拆分
func splitPart(partChan chan string) {
	getPartNameSql := "select partition_name from INFORMATION_SCHEMA.PARTITIONS where TABLE_SCHEMA = '" + Table.Owner + "' and TABLE_NAME = '" + Table.Name + "'"

	rows, err := MysConn.Query(getPartNameSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		var partName string

		err = rows.Scan(&partName)
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}
		partChan <- partName
	}

	return
}
