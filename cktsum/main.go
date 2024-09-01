// 一个异构数据库数据校验工具

package main

import (
	"cktsum/cmd"
	"cktsum/common"
)

func main() {
	cmd.ParseArgs()
	if common.CMode == "all" {
		cmd.Compredate()
	} else if common.CMode == "count" {
		cmd.CompreCnt()
	}
}
