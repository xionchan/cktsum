// 计算Oracle的校验和时一些公共函数

package oraclefunc

import (
	"cktsum/common"
	"github.com/shopspring/decimal"
	"log"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

// 并行计算crc32
func GetCrc32Sum() decimal.Decimal {
	rowidrChan := make(chan [2]string, common.Parallel+1) // 存放Oracle的rowid
	crc32Chan := make(chan decimal.Decimal, 100000)       // 存放一个分片的crc32和

	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup

	wg1.Add(1)
	go func() {
		defer wg1.Done()
		splistTab(rowidrChan)
	}()

	for i := 0; i < common.Parallel; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			if common.OraMode == "db" {
				dbGetCrc32(rowidrChan, crc32Chan)
			} else {
				extGetCrc32(rowidrChan, crc32Chan)
			}
		}()
	}

	wg1.Wait()
	close(rowidrChan)
	wg2.Wait()
	close(crc32Chan)

	totalCrc32Sum := decimal.NewFromFloat(0.0)
	for crc := range crc32Chan {
		totalCrc32Sum = totalCrc32Sum.Add(crc)
	}

	return totalCrc32Sum
}

// 根据rowid拆分表
func splistTab(rowidr chan [2]string) {
	// 获取对象的行数
	var getTabRowSql string
	var getExtentRowidSql string // 每个extents的rowid范围
	if PartMode {
		// 获取分区名字： 括号内的字符串，并转换为大写
		re := regexp.MustCompile(`\(([^)]+)\)`)
		partName := re.FindStringSubmatch(Wherec)
		getTabRowSql = "select coalesce(num_rows, 0) from all_tab_partitions where table_name = '" + Table.Name + "' and table_owner = '" +
			Table.Owner + "' and partition_name = '" + strings.ToUpper(partName[1]) + "'"

		getExtentRowidSql = `select dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id, 0), 
       							 dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id + blocks - 1, 32767) 
						  from dba_extents a, all_objects b
						  where a.segment_name = b.object_name  AND (a.PARTITION_NAME is null or a.PARTITION_NAME = b.SUBOBJECT_NAME)  
						  and a.owner = b.owner and a.owner = '` + Table.Owner + "' and a.segment_name = '" + Table.Name + "' and a.partition_name = '" + strings.ToUpper(partName[1]) + "'"
	} else {
		getTabRowSql = "select coalesce(num_rows, 0) from all_tables where table_name = '" + Table.Name + "' and owner = '" + Table.Owner + "'"

		getExtentRowidSql = `select dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id, 0), 
       							 dbms_rowid.rowid_create(1, data_object_id, relative_fno, block_id + blocks - 1, 32767) 
						  from dba_extents a, all_objects b
						  where a.segment_name = b.object_name  AND (a.PARTITION_NAME is null or a.PARTITION_NAME = b.SUBOBJECT_NAME)  
						  and a.owner = b.owner and a.owner = '` + Table.Owner + "' and a.segment_name = '" + Table.Name + "'"
	}

	var tableRows uint
	err := OraConn.QueryRow(getTabRowSql).Scan(&tableRows)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	if tableRows < common.RowCount {
		tableRows = common.RowCount
	}

	// 如果小于100W, 或者并行度为1, 那么不拆分
	if tableRows < 100*10000 || common.Parallel == 1 {
		ParaMode = false
		rowidr <- [2]string{"", ""}
		return
	}

	// 获取每个extents的rowid范围
	rows, err := OraConn.Query(getExtentRowidSql)
	if err != nil {
		_, file, line, _ := runtime.Caller(0)
		log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
	}

	for rows.Next() {
		var rowidB, rowidE string
		err := rows.Scan(&rowidB, &rowidE)
		if err != nil {
			_, file, line, _ := runtime.Caller(0)
			log.Fatalf("程序错误(%s) : 报错位置 %s:%d (%s) \n", Table.Owner+"."+Table.Name, file, line, err.Error())
		}
		rowidr <- [2]string{rowidB, rowidE}
	}

	return
}

// 生成SQL语句
func genQuerySql() string {
	// 定义列名，列类型
	type colType struct {
		colName string
		colType string
	}

	var colTypeInfo []colType

	// 获取oracle的列类型
	colTypeSql := "select column_name, data_type from all_tab_cols where owner = '" + Table.Owner + "' and table_name = '" +
		Table.Name + "' and column_name in ('" + strings.Join(*Collist, "','") + "') order by column_name"

	rows, err := OraConn.Query(colTypeSql)
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
	oCrc32Sql := ""
	oVcharS := []string{"VARCHAR2", "NVARCHAR2", "LONG", "DATE", "TIMESTAMP\\(\\d\\)", "TIMESTAMP\\(\\d\\) WITH TIME ZONE", "TIMESTAMP\\(\\d\\) WITH LOCAL TIME ZONE",
		"INTERVAL YEAR\\(\\d\\) TO MONTH", "INTERVAL DAY\\(\\d\\) TO SECOND", "ROWID", "CLOB", "NCLOB", "JSON", "BOOLEAN"}
	// 初始化varchar
	oVarcharStr := ""
	for _, ct := range colTypeInfo {
		for _, char := range oVcharS {
			reg := regexp.MustCompile(char)
			if reg.MatchString(ct.colType) {
				oVarcharStr = oVarcharStr + ct.colName + "||"
				break
			}
		}
	}

	// 初始化char : 需要截断右边的空格
	oCharStr := ""
	for _, ct := range colTypeInfo {
		if ct.colType == "char" || ct.colType == "nchar" {
			oCharStr = oCharStr + "trim(" + ct.colName + ")||"
		}
	}

	// 初始化二进制
	oBlobStr := ""
	for _, ct := range colTypeInfo {
		if ct.colType == "RAW" || ct.colType == "LONG RAW" || ct.colType == "BLOB" {
			if common.OraMode == "db" {
				oBlobStr = oBlobStr + "CAL_BLOB_CRC(" + ct.colName + ") + "
			} else {
				oBlobStr = oBlobStr + ct.colName + ","
			}
		}
	}

	// 初始化数字
	oNumStr := ""
	for _, ct := range colTypeInfo {
		if ct.colType == "NUMBER" || ct.colType == "FLOAT" || ct.colType == "BINARY_FLOAT" || ct.colType == "BINARY_DOUBLE" {
			oNumStr = oNumStr + "nvl(" + ct.colName + ",0) + "
		}
	}

	// 根据db或external构造不同的SQL
	if common.OraMode == "db" {
		oCrc32Sql = "select sum(0"
		if oVarcharStr != "" {
			if oCharStr != "" {
				oCrc32Sql = oCrc32Sql + " + CAL_STR_CRC(" + oVarcharStr + oCharStr[:len(oCharStr)-2] + ")"
			} else {
				oCrc32Sql = oCrc32Sql + " + CAL_STR_CRC(" + oVarcharStr[:len(oVarcharStr)-2] + ")"
			}
		} else {
			if oCharStr != "" {
				oCrc32Sql = oCrc32Sql + " + CAL_STR_CRC(" + oCharStr[:len(oCharStr)-2] + ")"
			}
		}

		if oBlobStr != "" {
			oCrc32Sql = oCrc32Sql + "+" + oBlobStr[:len(oBlobStr)-2]
		}

		if oNumStr != "" {
			oCrc32Sql = oCrc32Sql + "+" + oNumStr[:len(oNumStr)-2]
		}

		oCrc32Sql = oCrc32Sql + ")"
	} else {
		oCrc32Sql = "select "
		if oVarcharStr != "" {
			if oCharStr != "" {
				oCrc32Sql = oCrc32Sql + oVarcharStr + oCharStr[:len(oCharStr)-2]
			} else {
				oCrc32Sql = oCrc32Sql + oVarcharStr[:len(oVarcharStr)-2]
			}
		} else {
			if oCharStr != "" {
				oCrc32Sql = oCrc32Sql + oCharStr[:len(oCharStr)-2]
			}
		}

		if oBlobStr != "" {
			oCrc32Sql = oCrc32Sql + "," + oBlobStr[:len(oBlobStr)-1]
		}

		if oNumStr != "" {
			oCrc32Sql = oCrc32Sql + "," + oNumStr[:len(oNumStr)-2]
		}
	}

	return oCrc32Sql + " from " + Table.Owner + "." + Table.Name + " " + Wherec
}
