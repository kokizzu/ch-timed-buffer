package main

import (
	"fmt"
	"time"

	chBuffer "github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
)

// how to run manually (since signal cannot be properly sent using exe.Process.Signal):
//   go run pkg/wrapper/clickhouse/3signal_test/signal_main.go
// on other terminal in the middle of process (send SIGTERM):
//   killall signal_main
func main() {
	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := chBuffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)
	tb.ForceExitOnSignal = true

	go func() {
		time.Sleep(3*time.Second + 100*time.Millisecond)
		tb.Close() // kill anyway after 3 seconds if no signal/interrupt from outside
	}()

	for z := 0; z < 30; z++ {
		time.Sleep(100 * time.Millisecond)
		tb.Insert(shared.InsertValues(&start, z))
		fmt.Println(`Insert`, z)
		//fmt.Println(z)
	}

	<-tb.WaitFinalFlush
}
