// 校验行数, 校验crc32和

package cmd

import (
	"cktsum/common"
	"cktsum/mysqlfunc"
	"cktsum/oraclefunc"
	"fmt"
	"github.com/shopspring/decimal"
	"os"
	"sync"
)

// 校验源端和目标端数据是否一致，先对比行数，在对比数据校验和
func Compredate() {

	// 对比行数
	var sourceCount, targetCount uint

	var sourcewg sync.WaitGroup
	var targetwg sync.WaitGroup

	sourcewg.Add(1)
	go func() {
		defer sourcewg.Done()
		if common.SDSN.Type == "oracle" {
			sourceCount = oraclefunc.GetCount()
		} else if common.SDSN.Type == "mysql" {
			sourceCount = mysqlfunc.GetCount()
		}
	}()

	targetwg.Add(1)
	go func() {
		defer targetwg.Done()
		if common.TDSN.Type == "oracle" {
			targetCount = oraclefunc.GetCount()
		} else if common.TDSN.Type == "mysql" {
			targetCount = mysqlfunc.GetCount()
		}
	}()

	sourcewg.Wait()
	targetwg.Wait()
	if sourceCount != targetCount {
		fmt.Printf("校验失败 : 源端和目标端的总行数不一致 (%s:%d - %s:%d)\n", common.ST.Owner+"."+common.ST.Name, sourceCount, common.TT.Owner+"."+common.TT.Name, targetCount)
		os.Exit(1)
	}

	if sourceCount == 0 {
		fmt.Printf("校验校验成功 : %s - %s\n", common.ST.Owner+"."+common.ST.Name, common.TT.Owner+"."+common.TT.Name)
		return
	}

	// 更新行数
	common.RowCount = sourceCount

	// 对比校验和
	sourceCrcSum := decimal.NewFromFloat(0.0)
	targetCrcSum := decimal.NewFromFloat(0.0)

	sourcewg.Add(1)
	go func() {
		defer sourcewg.Done()
		if common.SDSN.Type == "oracle" {
			sourceCrcSum = oraclefunc.GetCrc32Sum()
		} else if common.TDSN.Type == "mysql" {
			sourceCrcSum = mysqlfunc.GetCrc32Sum()
		}
	}()

	targetwg.Add(1)
	go func() {
		defer targetwg.Done()
		if common.TDSN.Type == "oracle" {
			targetCrcSum = oraclefunc.GetCrc32Sum()
		} else if common.TDSN.Type == "mysql" {
			targetCrcSum = mysqlfunc.GetCrc32Sum()
		}
	}()
	sourcewg.Wait()
	targetwg.Wait()

	if !sourceCrcSum.Equal(targetCrcSum) {
		fmt.Printf("校验失败 : 源端(%s)和目标端(%s)的校验和不一致 (%s:%s - %s:%s)\n", common.SDSN.Type, common.TDSN.Type,
			common.ST.Owner+"."+common.ST.Name, sourceCrcSum.StringFixed(4), common.TT.Owner+"."+common.TT.Name, targetCrcSum.StringFixed(4))
		os.Exit(1)
	} else {
		fmt.Printf("校验成功 : 源端(%s)和目标端(%s)的校验和一致 (%s:%s - %s:%s)\n", common.SDSN.Type, common.TDSN.Type,
			common.ST.Owner+"."+common.ST.Name, sourceCrcSum.StringFixed(4), common.TT.Owner+"."+common.TT.Name, targetCrcSum.StringFixed(4))
		// fmt.Printf("校验校验成功 : %s - %s\n", common.ST.Owner+"."+common.ST.Name, common.TT.Owner+"."+common.TT.Name)
	}
}
