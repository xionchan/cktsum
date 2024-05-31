package main

import (
    "database/sql"
    "fmt"
    "log"

    "github.com/shopspring/decimal"
    _ "github.com/go-sql-driver/mysql"
)

func main() {
    // 数据库连接信息
    dsn := "chenxw:cQrcb.3f0@tcp(127.0.0.1:4001)/chenxw"
    db, err := sql.Open("mysql", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // 查询带有精度的数字
    var valueStr sql.NullString
    err = db.QueryRow("select crc32(v1) from chenxw where id = ?", 500000000).Scan(&valueStr)
    if err != nil {
        log.Fatal(err)
    }

    // 使用 decimal.Decimal 存储高精度数字
    var decimalValue decimal.Decimal
    if valueStr.Valid {
        decimalValue, err = decimal.NewFromString(valueStr.String)
        if err != nil {
            log.Fatal(err)
        }
    } else {
        fmt.Println("Value is NULL")
        return
    }

    fmt.Printf("Value: %s\n", decimalValue.StringFixed(4))
}

