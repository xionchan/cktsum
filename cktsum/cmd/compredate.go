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
	"time"
)

// 校验源端和目标端数据是否一致，先对比行数，在对比数据校验和
func Compredate() {

	var sourcewg sync.WaitGroup
	var targetwg sync.WaitGroup
	var coordwg sync.WaitGroup

	// 对比行数
	var sourceCount, targetCount uint // 存储行count
	var sourceCDone, targetCDone bool // 存储子任务完成的标识
	sourcewg.Add(1)
	go func() {
		defer sourcewg.Done()
		if common.SDSN.Type == "oracle" {
			sourceCount = oraclefunc.GetCount()
		} else if common.SDSN.Type == "mysql" {
			sourceCount = mysqlfunc.GetCount()
		}
		sourceCDone = true
	}()

	targetwg.Add(1)
	go func() {
		defer targetwg.Done()
		if common.TDSN.Type == "oracle" {
			targetCount = oraclefunc.GetCount()
		} else if common.TDSN.Type == "mysql" {
			targetCount = mysqlfunc.GetCount()
		}
		targetCDone = true
	}()

	// 对比校验和
	sourceCrcSum := decimal.NewFromFloat(0.0)
	targetCrcSum := decimal.NewFromFloat(0.0)
	var sCrc32Done, tCrc32Done bool // 存储子任务完成的标识

	sourcewg.Add(1)
	go func() {
		defer sourcewg.Done()
		if common.SDSN.Type == "oracle" {
			sourceCrcSum = oraclefunc.GetCrc32Sum()
		} else if common.TDSN.Type == "mysql" {
			sourceCrcSum = mysqlfunc.GetCrc32Sum()
		}
		sCrc32Done = true
	}()

	targetwg.Add(1)
	go func() {
		defer targetwg.Done()
		if common.TDSN.Type == "oracle" {
			targetCrcSum = oraclefunc.GetCrc32Sum()
		} else if common.TDSN.Type == "mysql" {
			targetCrcSum = mysqlfunc.GetCrc32Sum()
		}
		tCrc32Done = true
	}()

	// 协调进程
	coordwg.Add(1)
	go func() {
		defer coordwg.Done()
		for {
			time.Sleep(100 * time.Millisecond)
			if sourceCDone && targetCDone {
				elapsedSeconds := time.Now().Sub(common.StartTime).Seconds()
				if sourceCount != targetCount {
					fmt.Printf("校验失败,耗时(%.2f秒) : 源端(%s)和目标端(%s)的总行数不一致 (%s:%d - %s:%d)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
						common.ST.Owner+"."+common.ST.Name, sourceCount, common.TT.Owner+"."+common.TT.Name, targetCount)
					os.Exit(0)
				} else {
					if sourceCount == 0 {
						fmt.Printf("校验成功,耗时(%.2f秒) : 源端(%s)和目标端(%s)为空表 (%s - %s)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
							common.ST.Owner+"."+common.ST.Name, common.TT.Owner+"."+common.TT.Name)
						os.Exit(0)
					}
				}
			}

			if sCrc32Done && tCrc32Done {
				elapsedSeconds := time.Now().Sub(common.StartTime).Seconds()
				if !sourceCrcSum.Equal(targetCrcSum) {
					if sourceCDone && targetCDone {
						if sourceCount == targetCount {
							fmt.Printf("校验失败,耗时(%.2f秒) : 源端(%s)和目标端(%s)的校验和不一致 (%s:%s - %s:%s)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
								common.ST.Owner+"."+common.ST.Name, sourceCrcSum.StringFixed(4), common.TT.Owner+"."+common.TT.Name, targetCrcSum.StringFixed(4))
							os.Exit(0)
						} else {
							fmt.Printf("校验失败,耗时(%.2f秒) : 源端(%s)和目标端(%s)的总行数不一致 (%s:%d - %s:%d)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
								common.ST.Owner+"."+common.ST.Name, sourceCount, common.TT.Owner+"."+common.TT.Name, targetCount)
							os.Exit(0)
						}
					}
				} else {
					fmt.Printf("校验成功,耗时(%.2f秒) : 源端(%s)和目标端(%s)的校验和一致 (%s:%s - %s:%s)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
						common.ST.Owner+"."+common.ST.Name, sourceCrcSum.StringFixed(4), common.TT.Owner+"."+common.TT.Name, targetCrcSum.StringFixed(4))
					os.Exit(0)
				}
			}
		}
	}()

	// 等待子进程完成
	sourcewg.Wait()
	targetwg.Wait()
	coordwg.Wait()
}

// 校验行数
func CompreCnt() {
	var sourcewg sync.WaitGroup
	var targetwg sync.WaitGroup

	// 对比行数
	var sourceCount, targetCount uint // 存储行count
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

	elapsedSeconds := time.Now().Sub(common.StartTime).Seconds()

	if sourceCount != targetCount {
		fmt.Printf("校验失败,耗时(%.2f秒) : 源端(%s)和目标端(%s)的总行数不一致 (%s:%d - %s:%d)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
			common.ST.Owner+"."+common.ST.Name, sourceCount, common.TT.Owner+"."+common.TT.Name, targetCount)
		os.Exit(0)
	} else {
		fmt.Printf("校验成功,耗时(%.2f秒) : 源端(%s)和目标端(%s)的总行数一致 (%s:%d - %s:%d)\n", elapsedSeconds, common.SDSN.Type, common.TDSN.Type,
			common.ST.Owner+"."+common.ST.Name, sourceCount, common.TT.Owner+"."+common.TT.Name, targetCount)
		os.Exit(0)
	}
}
