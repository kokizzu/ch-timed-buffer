package shared

import (
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/kokizzu/ch-timed-buffer"
	"github.com/kokizzu/gotro/I"

	"github.com/kokizzu/gotro/L"
	"github.com/kokizzu/gotro/S"
)

const DateFormat = `2006-01-02`

func getNo(no []int) string {
	noStr := `1`
	if len(no) > 0 {
		noStr = I.ToStr(no[0])
	}
	return noStr
}

func InitTableAndTruncate(conn *sql.DB, no ...int) {
	noStr := getNo(no)
	dropSchema := `DROP TABLE IF EXISTS dummy` + noStr
	_, err := chBuffer.TraceSqlExec(conn, dropSchema)
	L.IsError(err, `failed drop table dummy`+noStr)

	dummySchema := `
CREATE TABLE IF NOT EXISTS dummy` + noStr + `
( strCol String
, intCol UInt64
, floatCol Float32 
, dateCol Date
, timeCol DateTime
) ENGINE=ReplacingMergeTree()
PARTITION BY modulo( intCol, 1000 )
PRIMARY KEY intCol
ORDER BY (intCol, timeCol)
`
	_, err = chBuffer.TraceSqlExec(conn, dummySchema)
	L.IsError(err, `failed create table dummy`+noStr)

	_, err = chBuffer.TraceSqlExec(conn, `TRUNCATE TABLE dummy`+noStr)
	L.IsError(err, `failed truncate table dummy`+noStr)

}

func PrepareFunc(tx *sql.Tx) *sql.Stmt {
	return PrepareFuncWithNo(tx)
}

func PrepareFuncWithNo(tx *sql.Tx, no ...int) *sql.Stmt {
	noStr := getNo(no)
	stmt, err := tx.Prepare(`INSERT INTO dummy` + noStr + `(strCol,intCol,floatCol,dateCol,timeCol) VALUES(?,?,?,?,?)`)
	L.IsError(err, `failed prepare insert to dummy`+noStr)
	return stmt
}

func InsertValues(t *time.Time, z int) []interface{} {
	return []interface{}{
		S.EncodeCB63(int64(z), 1),
		z,
		t.Sub(time.Now()).Seconds(),
		t.Format(DateFormat),
		t.Format(`2006-01-02 15:04:05`),
	}
}

func ConnectClickhouse() *sql.DB {
	conn, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	L.IsError(err, `failed connect to clickhouse`)
	return conn
}

func DummyCount(conn *sql.DB, no ...int) int {
	noStr := getNo(no)
	row := conn.QueryRow(`SELECT COUNT(1) FROM dummy` + noStr)
	count := 0
	err := row.Scan(&count)
	L.IsError(err, `failed scan dummy count`)
	return count
}
