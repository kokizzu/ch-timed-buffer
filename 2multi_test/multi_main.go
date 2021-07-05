package main

import (
	"sync"
	"time"

	"github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
)

// insert 25 x 10 in parallel
func main() {
	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := ch_timed_buffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)

	wg := sync.WaitGroup{}
	const ParallelCount = 10
	const RecordCount = 25
	const ShiftCount = 100 // make sure id not duplicate
	for z := 0; z < ParallelCount; z++ {
		wg.Add(1)
		go func(goroutineId int) {
			for z := 0; z < RecordCount; z++ {
				tb.Insert(shared.InsertValues(&start, goroutineId+z))
				//fmt.Println(z)
			}
			wg.Done()
		}(z * ShiftCount)
	}

	wg.Wait()

	tb.Close()

	<-tb.WaitFinalFlush
}
