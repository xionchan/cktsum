// 定义一些公共函数

package common

import (
	"database/sql"
	"fmt"
	"hash/crc32"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
)

// 校验连接串
func ValidDSN(dsn string) (string, string, string, string, string, string, error) {
	/*
	   校验数据库的连接信息串格式 ：dbtype@username/password@ip:port/service_name
	   1.第一个"@"字符之前是数据库类型 ：oracle, mysql, postgresql之一，不区分大小写,存储到变量dbtype里。
	   2.然后是用户名字，由字母,数字,下划线组成的字符串，必须以字母开头，不能以下划线结尾，存储到变量username里.
	   3.然后是密码，任意字符，需要贪婪匹配到最后一个"@"符号之前， 存储到password
	   4.然后是IP地址，必须匹配IP地址的格式，匹配到符号":"之前， 存储到host
	   5.然后是端口，数字，1000-65535, 匹配到符号"/"之前, 存储到port
	   6.最后是服务名，由字母,数字,下划线组成的字符串，必须以字母开头，不能以下划线结尾，存储到servicename
	*/
	re := regexp.MustCompile(`^(?i)(oracle|mysql|postgresql)@([a-zA-Z][_a-zA-Z0-9]*[a-zA-Z0-9])/(.*)@(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{4,5})/([a-zA-Z][_a-zA-Z0-9]*[a-zA-Z0-9])$`)
	matches := re.FindStringSubmatch(dsn)

	if len(matches) == 0 {
		return "", "", "", "", "", "", fmt.Errorf("无效的数据库连接串: %s", dsn)
	} else {
		return strings.ToLower(matches[1]), matches[2], matches[3], matches[4], matches[5], matches[6], nil
	}
}

// 获取Table结构
func GetTableName(tablename string) (owner string, name string, err error) {
	/*
		首先校验格式是否正确 ：
		字符串最多只能包含一个小数点
		1. 如果包含一个小数点，那么 ：
			a. 小数点之前的字符串，只能以字母或者双引号开头，如果以双引号开头，那么也必须以双引号结束； 双引号之前的一个字符必须是数字或字母
			b. 小数点之后的字符串，只能以字母或者双引号开头，如果以双引号开头，那么也必须以双引号结束； 双引号之前的一个字符必须是数字或字母
		2. 如果不包含小数点，那么 ：字符串，只能以字母或者双引号开头，如果以双引号开头，那么也必须以双引号结束； 双引号之前的一个字符必须是数字或字母
	*/
	re := regexp.MustCompile(`^(\"[a-zA-Z][a-zA-Z0-9_]*\"|[a-zA-Z][a-zA-Z0-9_]*)$`)

	dcnt := strings.Count(tablename, ".")

	if dcnt > 1 {
		return "", "", fmt.Errorf("程序错误: 表名格式错误 (%s)", tablename)
	} else if dcnt == 0 {
		if !re.MatchString(tablename) {
			return "", "", fmt.Errorf("程序错误: 表名格式错误 (%s)", tablename)
		} else {
			// 如果是双引号引起来的，直接返回
			if strings.Count(tablename, `"`) > 0 {
				return "", tablename, nil
			}
			// 如果没有引起来，那么转换为大写返回
			return "", strings.ToUpper(tablename), nil
		}
	} else {
		didx := strings.Index(tablename, ".")
		// 获取用户名
		if !re.MatchString(tablename[:didx]) {
			return "", "", fmt.Errorf("程序错误: 表名格式错误 (%s)", tablename)
		} else {
			// 如果有双引号
			if strings.Count(tablename[:didx], `"`) > 0 {
				owner = tablename[:didx]
			} else {
				owner = strings.ToUpper(tablename[:didx])
			}
		}

		// 获取表名
		if !re.MatchString(tablename[didx+1:]) {
			return "", "", fmt.Errorf("程序错误: 表名格式错误 (%s)", tablename)
		} else {
			// 如果有双引号
			if strings.Count(tablename[didx+1:], `"`) > 0 {
				name = tablename[didx+1:]
			} else {
				name = strings.ToUpper(tablename[didx+1:])
			}
		}
	}

	return owner, name, nil
}

// 连接数据库
func CreateDbConn(dbdsn DBConnection) (*sql.DB, error) {
	/*
		根据不同的数据库种类，以及连接串，创建数据库连接，并将连接返回
	*/
	var err error
	var db *sql.DB

	if dbdsn.Type == "oracle" {
		dsn := `user="` + dbdsn.User + `" password="` + dbdsn.Password + `" connectString="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=` + dbdsn.Host +
			`)(PORT=` + dbdsn.Port + `))(CONNECT_DATA=(SERVICE_NAME=` + dbdsn.Database + `)))" heterogeneousPool=false standaloneConnection=true timezone="+08:00"`
		// dsn := dbdsn.User + "/" + dbdsn.Password + "@" + dbdsn.Host + ":" + dbdsn.Port + "/" + dbdsn.Database
		db, err = sql.Open("godror", dsn)
	} else if dbdsn.Type == "mysql" {
		dsn := dbdsn.User + ":" + dbdsn.Password + "@tcp(" + dbdsn.Host + ":" + dbdsn.Port + ")/" + dbdsn.Database + "?parseTime=true&loc=Local"
		db, err = sql.Open("mysql", dsn)
	}

	if err != nil {
		return nil, fmt.Errorf("程序错误: 连接数据库失败 (%s)", err.Error())
	}

	return db, nil
}

// 将字符串转换数组，以逗号分割。包含某个字符时，字符串大写或小写。
func ConvStrToSli(str string, format string, constr string) []string {
	elements := strings.Split(str, ",")
	results := make([]string, len(elements))
	for _, element := range elements {
		if strings.Contains(element, constr) {
			results = append(results, element)
		} else {
			if format == "upper" {
				results = append(results, strings.ToUpper(element))
			} else if format == "lower" {
				results = append(results, strings.ToLower(element))
			}
		}
	}

	return results
}

func AreValuesEqual(a, b interface{}) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false // 不同类型的值不相等
	}

	valueA := reflect.ValueOf(a)
	valueB := reflect.ValueOf(b)

	switch valueA.Kind() {
	case reflect.Array, reflect.Slice:
		if valueA.Len() != valueB.Len() {
			return false
		}
		for i := 0; i < valueA.Len(); i++ {
			if !AreValuesEqual(valueA.Index(i).Interface(), valueB.Index(i).Interface()) {
				return false
			}
		}
	case reflect.Map:
		if valueA.Len() != valueB.Len() {
			return false
		}
		for _, key := range valueA.MapKeys() {
			if !AreValuesEqual(valueA.MapIndex(key).Interface(), valueB.MapIndex(key).Interface()) {
				return false
			}
		}
	default:
		return reflect.DeepEqual(a, b)
	}

	return true
}

// 对比两个切片是否一致
func AreSliceEqual(s1, s2 []string, casesen bool) bool {
	// 排序
	sort.Strings(s1)
	sort.Strings(s2)

	if casesen {
		return reflect.DeepEqual(s1, s2)
	} else {
		for i, str := range s1 {
			s1[i] = strings.ToUpper(str)
		}

		for i, str := range s2 {
			s2[i] = strings.ToUpper(str)
		}
		return reflect.DeepEqual(s1, s2)
	}
}

// 计算二进制数据的crc32
func ComputeCrc(data []byte) uint64 {
	hash := crc32.NewIEEE()
	hash.Write(data)
	checksum := uint64(hash.Sum32())
	return checksum
}

// 获取参数
func GetVar(sourcet string) (DBConnection, Table, string, *[]string) {
	if sourcet == "source" {
		return SDSN, ST, SWhere, &SColList
	} else {
		return TDSN, TT, TWhere, &TColList
	}
}

// 转换列信息 : 某个符号引起来的字符串不转换，其他字符转换为大写或者小写
func ConvStr(str, syn, capi string) []string {
	var restr []string

	arr := strings.Split(str, ",")
	for _, element := range arr {
		if strings.Contains(element, syn) {
			restr = append(restr, strings.ReplaceAll(strings.ReplaceAll(element, " ", ""), syn, ""))
		} else {
			if capi == "upper" {
				restr = append(restr, strings.ReplaceAll(strings.ToUpper(element), " ", ""))
			} else {
				restr = append(restr, strings.ReplaceAll(strings.ToUpper(element), " ", ""))

			}
		}
	}

	return restr
}
