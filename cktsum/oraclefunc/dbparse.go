// 入库校验参数的合法性

package oraclefunc

import (
	"cktsum/common"
	"database/sql"
	"log"
	"os"
	"strings"
)

var SourceType string       // 源库还是目标库
var PartMode bool           // 是否是分区模式
var OraConn *sql.DB         // 初始化数据库连接
var Table common.Table      // 用户名.表名
var Wherec string           // where条件
var Collist *[]string       // 列列表的指针
var Dsn common.DBConnection // 数据库的连接信息
var ParaMode bool = true    // 是否是并行模式

// 建立数据库连接， 初始化局部全局参数
func DbParse(sourcet string) []string {
	SourceType = sourcet
	if SourceType == "source" {
		PartMode = common.SPartMode
	} else {
		PartMode = common.TPartMode
	}

	Dsn, Table, Wherec, Collist = common.GetVar(SourceType)
	// 校验数据库连通性，以及可以正常执行SQL语句
	db, err := common.CreateDbConn(Dsn)
	if err != nil {
		log.Fatal(err)
	}
	OraConn = db

	dbcheck()

	return *Collist
}

// Oracle入库校验函数， 如果没有colstr, 那么需要校验col
func dbcheck() {
	/*
		1.检查oracle数据库的表是否存在
		2.检查colStr的合法性
		3.检查where条件的合法性
		4.oraMode为db时, 校验是否需要更新
		5.更新并行度
	*/
	_, err := OraConn.Exec("select 1 from dual")
	if err != nil {
		log.Println("程序错误 : oracle无法执行SQL语句 (%s)", err.Error())
		os.Exit(1)
	}

	// 判断表是否存在
	_, err = OraConn.Exec("select 1 from " + Table.Owner + "." + Table.Name + " where 1=0")
	if err != nil {
		log.Println("程序错误 : oracle中不存在该表 " + Table.Owner + "." + Table.Name)
		os.Exit(1)
	}

	// 检查colStr的合法性, 然后获取列信息
	var colsql string

	if common.ColStr != "" {
		_, err := OraConn.Exec("select " + common.ColStr + " from " + Table.Owner + "." + Table.Name + " where 1=0")
		if err != nil {
			log.Println("程序错误 : oracle传入的列信息有误 " + common.ColStr)
			os.Exit(1)
		}
		tempcollist := common.ConvStr(common.ColStr, `"`, "upper")
		colsql = "select column_name from all_tab_cols where owner = '" + Table.Owner + "' and table_name = '" + Table.Name +
			"' and column_name in ('" + strings.Join(tempcollist, "','") + "') order by column_name"
	} else { // 需要返回列进行对比
		colsql = "select column_name from all_tab_cols where owner = '" + Table.Owner + "' and table_name = '" + Table.Name +
			"' and column_name not like 'SYS\\_%$' order by column_name"
	}

	orarows, _ := OraConn.Query(colsql)

	for orarows.Next() {
		var colname string
		_ = orarows.Scan(&colname)
		*Collist = append(*Collist, colname)
	}

	// 检查where条件的合法性
	if Wherec != "" {
		var checkSql string
		if PartMode {
			checkSql = "select 1 from " + Table.Owner + "." + Table.Name + " " + Wherec + " where 1=0"
		} else {
			checkSql = "select 1 from " + Table.Owner + "." + Table.Name + " " + Wherec + " and 1=0"
		}
		_, err := OraConn.Exec(checkSql)
		if err != nil {
			log.Println("程序错误 : oracle传入的where条件有误 " + Table.Owner + "." + Table.Name)
			os.Exit(1)
		}
	}

	// 检查是否可以访问dba_extents
	if common.Parallel > 1 {
		_, err := OraConn.Exec("select 1 from dba_extents where 1 = 0")
		if err != nil {
			common.Parallel = 1
			log.Println("程序警告 : 由于Oracle用户没有访问DBA_EXTENTS视图的权限，并行度调整为1。")
		}
	}

	// 调整oramode
	if common.OraMode == "db" {
		var count int
		_ = OraConn.QueryRow("select count(*) from user_objects where object_name in ('CAL_BLOB_CRC', 'CAL_STR_CRC', 'CAL_CLOB_CRC') and status = 'VALID'").Scan(&count)
		if count != 3 {
			log.Println("程序警告 : 由于Oracle中没有存储过程CALTABCRC32, 转换为external模式计算校验和")
			common.OraMode = "external"
		}
	}
}
