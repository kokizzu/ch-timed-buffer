package main

import (
	"time"

	"github.com/kokizzu/ch-timed-buffer"
	"github.com/kokizzu/ch-timed-buffer/0shared_test"
)

// insert 105 items serially
func main() {
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := ch_timed_buffer.NewTimedBuffer(conn, 10, 100*time.Millisecond, shared.PrepareFunc)

	time.Sleep(1 * time.Second)

	tb.Close()
	<-tb.WaitFinalFlush
}
