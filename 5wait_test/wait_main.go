package main

import (
	"sync"
	"time"

	"github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
)

// insert 125 x 10 in parallel
func main() {
	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := chBuffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)
	tb.DontWaitMoreInsertAfterClose = true

	wg := sync.WaitGroup{}
	const ParallelCount = 10
	const RecordCount = 125
	const ShiftCount = 1000 // make sure id not duplicate
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

	tb.Insert(shared.InsertValues(&start, 99999)) // wil not be processed = data loss

	<-tb.WaitFinalFlush
}
