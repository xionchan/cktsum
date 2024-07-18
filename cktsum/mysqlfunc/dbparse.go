// mysql入库校验参数的合法性, 获取行数

package mysqlfunc

import (
	"cktsum/common"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
)

var SourceType string       // 源库还是目标库
var PartMode bool           // 是否是分区模式
var MysConn *sql.DB         // 初始化数据库连接
var Table common.Table      // 用户名.表名
var Wherec string           // where条件
var Collist *[]string       // 列列表的指针
var Dsn common.DBConnection // 数据库的连接信息
var ParaMode bool           // 是否并行模式

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
	MysConn = db

	dbcheck()

	checkPara()

	return *Collist
}

// mysql入库校验函数, 如果没有colstr, 那么需要校验col
func dbcheck() {
	/*
		1.检查mysql数据库的表是否存在
		2.检查colStr的合法性
		3.检查where条件的合法性
	*/
	_, err := MysConn.Exec("select 1")
	if err != nil {
		log.Println("程序错误 : mysql无法执行SQL语句 (%s)", err.Error())
		os.Exit(1)
	}

	// 判断表是否存在
	_, err = MysConn.Exec("select 1 from " + Table.Owner + "." + Table.Name + " where 1=0")
	if err != nil {
		log.Println("程序错误 : mysql中不存在该表 " + Table.Owner + "." + Table.Name)
		os.Exit(1)
	}

	// 检查colStr的合法性, 获取返回的列信息, 转换为小写，带反引号的字符串不变
	var colsql string

	if common.ColStr != "" {
		_, err := MysConn.Exec("select " + common.ColStr + " from " + Table.Owner + "." + Table.Name + " where 1=0")
		if err != nil {
			log.Println("程序错误 : mysql传入的列信息有误 " + common.ColStr)
			os.Exit(1)
		}
		tempcollist := common.ConvStr(common.ColStr, "`", "lower")
		colsql = "select column_name from information_schema.columns where table_schema = '" + Table.Owner + "' and table_name = '" + Table.Name +
			"' and column_name in ('" + strings.Join(tempcollist, "','") + "') order by column_name collate utf8_general_ci"
	} else { // 需要返回列进行对比
		colsql = "select column_name from information_schema.columns where table_schema = '" + Table.Owner + "' and table_name = '" + Table.Name +
			"' and not (column_key = 'PRI' and (extra in ('auto_increment', 'DEFAULT_GENERATED'))) order by column_name collate utf8_general_ci"
	}

	mysrows, _ := MysConn.Query(colsql)

	for mysrows.Next() {
		var colname string
		_ = mysrows.Scan(&colname)
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
		_, err := MysConn.Exec(checkSql)
		if err != nil {
			fmt.Println(checkSql)
			log.Println("程序错误 : mysql传入的where条件有误 " + Table.Owner + "." + Table.Name)
			os.Exit(1)
		}
	}
}

// 判断是否时并行模式
func checkPara() {
	// 如果并行是1， 那么直接返回
	if common.Parallel == 1 {
		ParaMode = false
		return
	}

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
	if tableRows < 100*10000 {
		ParaMode = false
	} else {
		ParaMode = true
	}

	return
}
