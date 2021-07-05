package shared

import (
	"database/sql"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	ch_timed_buffer "github.com/kokizzu/ch-timed-buffer"

	"github.com/kokizzu/gotro/L"
	"github.com/kokizzu/gotro/S"
)

func InitTableAndTruncate(conn *sql.DB) {
	const dropSchema = `DROP TABLE IF EXISTS dummy1`
	_, err := ch_timed_buffer.TraceSqlExec(conn, dropSchema)
	L.IsError(err, `failed drop table dummy1`)

	const dummySchema = `
CREATE TABLE IF NOT EXISTS dummy1
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
	_, err = ch_timed_buffer.TraceSqlExec(conn, dummySchema)
	L.IsError(err, `failed create table dummy1`)

	_, err = ch_timed_buffer.TraceSqlExec(conn, `TRUNCATE TABLE dummy1`)
	L.IsError(err, `failed truncate table dummy1`)
}

func PrepareFunc(tx *sql.Tx) *sql.Stmt {
	stmt, err := tx.Prepare(`INSERT INTO dummy1(strCol,intCol,floatCol,dateCol,timeCol) VALUES(?,?,?,?,?)`)
	L.IsError(err, `failed prepare insert to dummy1`)
	return stmt
}

func InsertValues(t *time.Time, z int) []interface{} {
	return []interface{}{
		S.EncodeCB63(int64(z), 1),
		z,
		t.Sub(time.Now()).Seconds(),
		t.Format(global.DateFormat),
		t.Format(`2006-01-02 15:04:05`),
	}
}

func ConnectClickhouse() *sql.DB {
	conn, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	L.IsError(err, `failed connect to clickhouse`)
	return conn
}

func DummyCount(conn *sql.DB) int {
	row := conn.QueryRow(`SELECT COUNT(1) FROM dummy1`)
	count := 0
	err := row.Scan(&count)
	L.IsError(err, `failed scan dummy count`)
	return count
}
