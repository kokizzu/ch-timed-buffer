package main

import (
	"time"

	chBuffer "github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
)

// insert 105 items serially
func main() {
	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := chBuffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)

	for z := 0; z < 105; z++ {
		tb.Insert(shared.InsertValues(&start, z))
		//fmt.Println(z)
	}

	tb.Close()
	<-tb.WaitFinalFlush
}
