// 参数校验函数, 初步校验，入库校验
package cmd

import (
	"cktsum/common"
	"cktsum/mysqlfunc"
	"cktsum/oraclefunc"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

// 重写Uasge
func usage() {
	// 重写Usage, 打印一些额外的信息
	fmt.Fprintf(os.Stderr, "\ncksumt : 一个用于校验数据库数据的工具.")
	fmt.Fprintf(os.Stderr, "\nCopyright (c) 2023, 2024, Cqrcb chenxw.")
	fmt.Fprintf(os.Stderr, "\n Usage: cksumt [options]\n")

	flag.PrintDefaults()

	fmt.Fprintf(os.Stderr, "\n附加信息:")
	fmt.Fprintf(os.Stderr, "\n 1. -b 和 -B 必须指定1个, 当指定2个中的1个时, 另一个继承指定的值")
	fmt.Fprintf(os.Stderr, "\n 2. -w 和 -W 指定其中1个时, 另一个继承指定的值")
	fmt.Fprintf(os.Stderr, "\n 3. 指定对象名(列名,表名), 如需保留原始大小写, 使用对应数据库应用方式, Oracle,pg为双引号,mysql为反引号")
	fmt.Fprintf(os.Stderr, "\n\n下面是一个例子 :")
	fmt.Fprintf(os.Stderr, "./cksumt -s oracle@scott/triger@192.168.1.1:1521/orcl -t mysql@demo/demo@192.168.1.1:3306/test -m external -c col1,col2,\"c3\" -p 12 -f 100000 -w \" where \"id\" in (select id from t1)\" -W \" where id > 100\" -b db.t1 -B db.t1\n")

	// 然后退出程序
	os.Exit(1)
}

// 参数校验函数
func ParseArgs() {
	// 初始化命令行参数
	sourceDsn := flag.String("s", "", "强制参数, 指定源库连接方式, [oracle|mysql|postgresql]@user/passwd@ip:port/dbname")
	targetDsn := flag.String("t", "", "强制参数, 指定目标库连接方式, [oracle|mysql|postgresql]@user/passwd@ip:port/dbname")
	oraMode := flag.String("m", "db", "可选参数, [db|external], Oracle数据库计算crc32的方式")
	colStr := flag.String("c", "", "可选参数, 指定列列表, 如果要保留原始大小写，使用双引号")
	dbPara := flag.Int("p", 1, "可选参数, 指定数据库的并行度, 最大16")
	fetchSize := flag.Int("f", 500000, "可选参数，指定每个分片的大小, 默认500000，过大可能导致内存溢出")
	sWhere := flag.String("w", "", "可选参数，源库的where条件")
	tWhere := flag.String("W", "", "可选参数，源库的where条件")
	sTable := flag.String("b", "", "可选参数, 指定源库表名, 默认转换为大写, 如果要保留原始大小写，使用双引号")
	tTable := flag.String("B", "", "可选参数, 指定目标库表名, 默认转换为大写, 如果要保留原始大小写，使用双引号")

	flag.Parse()
	flag.Usage = usage

	var err error
	// 强制参数sourceDsn, 校验格式, 并更新源库信息, 校验数据库的连通性
	if *sourceDsn == "" {
		log.Println("程序错误 : 缺少强制传入参数 -s !")
		flag.Usage()
	} else {
		common.SDSN.Type, common.SDSN.User, common.SDSN.Password, common.SDSN.Host, common.SDSN.Port, common.SDSN.Database, err = common.ValidDSN(*sourceDsn)
		if err != nil {
			log.Println("程序错误 : 无效的数据库连接串 %s", *sourceDsn)
			flag.Usage()
		}
	}

	// 强制参数targetDsn, 校验格式, 并更新源库信息
	if *targetDsn == "" {
		log.Println("程序错误 : 缺少强制传入参数 -s !")
		flag.Usage()
	} else {
		common.TDSN.Type, common.TDSN.User, common.TDSN.Password, common.TDSN.Host, common.TDSN.Port, common.TDSN.Database, err = common.ValidDSN(*targetDsn)
		if err != nil {
			log.Println("程序错误 : 无效的数据库连接串 %s", *targetDsn)
			flag.Usage()
		}
	}

	// 两个数据库不能是相同类型
	if common.SDSN.Type == common.TDSN.Type {
		log.Println("程序错误 : 源库和目标库的类型不能相同 !")
		flag.Usage()
	}

	// 检查表 ：必须传入sTable或tTable, 或者两者都传入, 并更新表名
	if *sTable == "" && *tTable == "" {
		log.Println("程序错误 : -b 和 -B 参数至少需要传入1个 !")
		flag.Usage()
	} else if *sTable != "" && *tTable != "" {
		common.TT.Owner, common.TT.Name, err = common.GetTableName(*tTable)
		if err != nil {
			log.Println(err)
			flag.Usage()
		}

		if common.TT.Owner == "" {
			common.TT.Owner = strings.ToUpper(common.TDSN.User)
		}

		common.ST.Owner, common.ST.Name, err = common.GetTableName(*sTable)
		if err != nil {
			log.Println(err)
			flag.Usage()
		}

		if common.ST.Owner == "" {
			common.ST.Owner = strings.ToUpper(common.SDSN.User)
		}
	} else {
		var tableName string
		if *sTable != "" {
			tableName = *sTable
		} else {
			tableName = *tTable
		}

		common.TT.Owner, common.TT.Name, err = common.GetTableName(tableName)
		if err != nil {
			log.Println(err)
			flag.Usage()
		}

		// 如果owner是空，那么owner就是连接的用户名
		if common.TT.Owner == "" {
			common.TT.Owner = strings.ToUpper(common.TDSN.User)
			common.ST.Owner = strings.ToUpper(common.SDSN.User)
		} else {
			common.ST.Owner = common.TT.Owner
		}

		// 源库的表名等于目标库的表名
		common.ST.Name = common.TT.Name
	}

	// 并行度
	if *dbPara > 16 {
		common.Parallel = 16
		log.Println("程序警告 : 并行度设置超过最大值16, 调整为16。")
	} else if *dbPara < 1 {
		common.Parallel = 1
		log.Println("程序警告 : 并行度设置小于1, 调整为1。")
	} else {
		common.Parallel = *dbPara
	}

	// 获取fetchsize
	if *fetchSize > 1 {
		common.FetchSize = *fetchSize
	} else {
		common.FetchSize = 10000
		log.Println("程序警告 : fetchsize调整为10000。")
	}

	// 获取Oracle的校验模式
	if *oraMode != "db" && *oraMode != "external" {
		fmt.Println("程序错误: Oracle的校验模式只能是 db 或者 external")
		flag.Usage()
	} else {
		common.OraMode = *oraMode
	}

	// 更新列信息参数
	if *colStr != "" {
		common.ColStr = *colStr
	} else {
		common.ColStr = ""
	}

	// 更新where信息
	if *tWhere != "" && *sWhere != "" {
		common.TWhere = strings.TrimSpace(*tWhere)
		common.SWhere = strings.TrimSpace(*sWhere)
	} else if *tWhere != "" {
		common.TWhere = strings.TrimSpace(*tWhere)
		common.SWhere = strings.TrimSpace(*tWhere)
	} else if *sWhere != "" {
		common.TWhere = strings.TrimSpace(*sWhere)
		common.SWhere = strings.TrimSpace(*sWhere)
	} else {
		common.TWhere = ""
		common.SWhere = ""
	}

	// 条件语句只能是以where或者partition开头
	if common.TWhere != "" {
		if !strings.HasPrefix(strings.ToLower(common.TWhere), "where") {
			if !strings.HasPrefix(strings.ToLower(common.TWhere), "partition") {
				fmt.Println("程序错误 : 传入的条件语句有误")
				flag.Usage()
			} else {
				if strings.Contains(strings.ToLower(common.TWhere), "where") {
					fmt.Println("程序错误 : 传入的条件语句有误")
					flag.Usage()
				}
				common.TPartMode = true
			}
		} else {
			common.TPartMode = false
		}
	}

	if common.SWhere != "" {
		if !strings.HasPrefix(strings.ToLower(common.SWhere), "where") {
			if !strings.HasPrefix(strings.ToLower(common.SWhere), "partition") {
				fmt.Println("程序错误 : 传入的条件语句有误")
				flag.Usage()
			} else {
				if strings.Contains(strings.ToLower(common.SWhere), "where") {
					fmt.Println("程序错误 : 传入的条件语句有误")
					flag.Usage()
				}
				common.SPartMode = true
			}
		} else {
			common.SPartMode = false
		}
	}

	dbParseArgs()
}

// 入库校验参数
func dbParseArgs() {
	var sourceList, targetList []string

	if common.SDSN.Type == "oracle" {
		sourceList = oraclefunc.DbParse("source")
	} else if common.SDSN.Type == "mysql" {
		sourceList = mysqlfunc.DbParse("source")
	}

	if common.TDSN.Type == "oracle" {
		targetList = oraclefunc.DbParse("target")
	} else if common.TDSN.Type == "mysql" {
		targetList = mysqlfunc.DbParse("target")
	}

	// 校验列
	if !common.AreSliceEqual(sourceList, targetList, false) {
		log.Println("对比失败 : 源端和目标端的列不一致!")
		os.Exit(1)
	}
}
