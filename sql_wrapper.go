package chBuffer

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
)

var workDir string
var exeDir string

func init() {
	workDir, _ = os.Getwd()
	idx := strings.Index(workDir, `/src/`)
	if idx >= 0 {
		workDir = workDir[:idx+5]
	}
	exeDir, _ = os.Executable()
	idx = strings.Index(exeDir, `/src/`)
	if idx >= 0 {
		exeDir = exeDir[:idx+5]
	}
}

func trimPath(fileName string, line int, funcName string) string {
	wdl := len(workDir)
	if len(fileName) > wdl {
		if fileName[:wdl] == workDir {
			fileName = fileName[wdl:]
		}
	}
	edl := len(exeDir)
	if len(fileName) > edl {
		if fileName[:edl] == exeDir {
			fileName = fileName[edl:]
		}
	}
	idx := strings.LastIndex(funcName, `/`)
	if idx > 0 && idx+1 < len(funcName) {
		funcName = funcName[idx+1:]
	}
	return fmt.Sprintf("%s:%d %s", fileName, line, funcName)
}

// when calling this, ensure that there's a newline (\n) before concatenation
func SqlCallerComment() string {
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		file, line := details.FileLine(pc)
		return fmt.Sprintf(`-- %s`, trimPath(file, line, details.Name()))
	}
	return `-- unknown caller`
}

func sqlReplacer(sql string, params ...interface{}) string {
	for _, param := range params {
		str, ok := param.(string)
		if ok {
			str = `'` + strings.Replace(str, `'`, `\'`, -1) + `'`
		} else {
			str = `'` + fmt.Sprint(param) + `'`
		}
		sql = strings.Replace(sql, `?`, str, 1)
	}
	return sql
}

func DebugMode() bool {
	return os.Getenv(`ENV`) == ``
}

func wsToSpace(x string) string {
	x = strings.ReplaceAll(x, "\n", "  ")
	x = strings.ReplaceAll(x, "\t", " ")
	return x
}

func LogSql(res interface{}, sql string, params ...interface{}) {
	if DebugMode() {
		sql := sqlReplacer(sql, params...)
		fmt.Println(`LOG-sql:
 ` + wsToSpace(sql) + `
-- result:
 ` + fmt.Sprintf("%#v", res))
	}
}

func TraceSqlExec(db *sql.DB, query string, params ...interface{}) (sql.Result, error) {
	if DebugMode() {
		fmt.Println(sqlReplacer(query, params...))
	}
	return db.ExecContext(context.Background(), query, params...)
}

func TraceSqlRows(db *sql.DB, query string, params ...interface{}) (*sql.Rows, error) {
	if DebugMode() {
		fmt.Println(sqlReplacer(query, params...))
	}
	return db.QueryContext(context.Background(), query, params...)
}

func TraceSqlRow(db *sql.DB, query string, params ...interface{}) *sql.Row {
	if DebugMode() {
		fmt.Println(sqlReplacer(query, params...))
	}
	return db.QueryRowContext(context.Background(), query, params...)
}
