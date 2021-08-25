package paralel_test

import (
	"database/sql"
	"testing"
	"time"

	chBuffer "github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
)

func PrepareFunc2(tx *sql.Tx) *sql.Stmt {
	return shared.PrepareFuncWithNo(tx, 2)
}

func Test_MultipleTimedBuffer(t *testing.T) {

	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)
	shared.InitTableAndTruncate(conn, 2)

	tb := chBuffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)
	tb2 := chBuffer.NewTimedBuffer(conn, 10, 1*time.Second, PrepareFunc2)

	for z := 0; z < 105; z++ {
		tb.Insert(shared.InsertValues(&start, z))
		tb2.Insert(shared.InsertValues(&start, z))
		//fmt.Println(z)
	}

	go func() {
		time.Sleep(1*time.Second + 100*time.Millisecond)
		tb.Close() // kill anyway after 1 seconds if no signal/interrupt from outside
		tb2.Close()
	}()

	<-tb2.WaitFinalFlush // order doesn't matter
	<-tb.WaitFinalFlush
}
