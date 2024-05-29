// 获取表的行数

package oraclefunc

// 获取对应表的行数
func GetCount() uint {
	var rowcount uint

	_ = OraConn.QueryRow("select count(*) from " + Table.Owner + "." + Table.Name + " " + Wherec).Scan(&rowcount)

	return rowcount
}
