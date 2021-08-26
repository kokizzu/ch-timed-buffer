package chBuffer

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/kokizzu/gotro/L"
)

type Buffer struct {
	Transaction *sql.Tx
	Statement   *sql.Stmt
	Counter     int
}

type TimedBuffer struct {
	Connection *sql.DB     // connection to clickhouse or any other database
	writeIdx   uint        // index of double buffer
	mutex      *sync.Mutex // so no insert and flush happened on the same time
	buffer     [2]*Buffer

	Preparator     Preparator // function (required) to create prepared statement from transaction
	OnExitCallback func()     // function (optional) that called when exit triggered and no more inserts

	TriggerForceFlush chan bool          // channel to trigger force flush
	TriggerExit       chan bool          // channel to trigger force exit, equal to .Close()
	insertQueue       chan []interface{} // channel to send data, equal to .Insert()

	WaitFinalFlush  chan bool // wait channel, to hold main function so it won't exit prematurely
	Debug           bool      // if true will print triggers
	IgnoreInterrupt bool      // if true, will not catch interrupt

	// init only
	maxBatch                     int           // maximum rows before flushing
	tickDuration                 time.Duration // maximum duration before flushing, should be <15s (half of default kube termination period)
	exitTriggered                bool          // whether exit already triggered
	ForceExitOnSignal            bool          // if true, then will call os.Exit(0) when buffer closed
	DontWaitMoreInsertAfterClose bool          // if true, then next Insert will not be processed after close
}

type Preparator func(conn *sql.Tx) *sql.Stmt

// 	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
func NewTimedBuffer(connect *sql.DB, maxBuffer int, tickDuration time.Duration, preparator Preparator) *TimedBuffer {
	res := TimedBuffer{
		Connection:        connect,
		Preparator:        preparator,
		mutex:             &sync.Mutex{},
		TriggerExit:       make(chan bool),
		TriggerForceFlush: make(chan bool),
		insertQueue:       make(chan []interface{}, maxBuffer),
		maxBatch:          maxBuffer,
		tickDuration:      tickDuration,
		WaitFinalFlush:    make(chan bool),
	}
	go res.Timer()
	if res.IgnoreInterrupt {
		go res.HandleTermSignal()
	}
	return &res
}

// if closed and DontWaitMoreInsertAfterClose=true data might be lost
func (t *TimedBuffer) Close() {
	t.TriggerExit <- true
}

// will return false if inserted after closed
func (t *TimedBuffer) Insert(args []interface{}) bool {
	t.insertQueue <- args
	if t.exitTriggered && t.DontWaitMoreInsertAfterClose {
		fmt.Println(`more insert called after close`)
		return false
	}
	return true
}

// returns true if full
func (t *TimedBuffer) insertTriggered(args []interface{}) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	buff := t.getBuffer(t.writeIdx % 2)
	//tracer.DumpNonProd(`args`,args)
	_, err := buff.Statement.Exec(args...)
	L.IsError(err, `failed statement exec`)
	buff.Counter++
	return buff.Counter >= t.maxBatch
}

func (t *TimedBuffer) Flush() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	idx := t.writeIdx % 2
	buff := t.buffer[idx]
	t.writeIdx++
	t.writeIdx %= 2
	if buff == nil {
		return
	}
	err := buff.Transaction.Commit()
	L.IsError(err, `failed transaction commit`)
	err = buff.Statement.Close()
	L.IsError(err, `failed statement close`)
	if t.Debug {
		fmt.Println(`buffer flushed`)
	}
	t.buffer[idx] = nil // switch buffer
}

func (t *TimedBuffer) getBuffer(idx uint) *Buffer {
	if t.buffer[idx] == nil {
		tx, err := t.Connection.Begin()
		if L.IsError(err, `failed transaction begin`) {
			return nil
		}
		stmt := t.Preparator(tx)
		t.buffer[idx] = &Buffer{
			Transaction: tx,
			Statement:   stmt,
			Counter:     0,
		}
	}
	return t.buffer[idx]
}

func (t *TimedBuffer) Timer() {
	defer func() {
		fmt.Println(`WaitFinalFlush done, no more insert will be handled`)
		if t.ForceExitOnSignal {
			os.Exit(0)
		} else {
			close(t.WaitFinalFlush)
		}
	}()
	ticker := time.NewTicker(t.tickDuration)
	for {
		select {
		case args := <-t.insertQueue:
			if t.Debug {
				fmt.Println(`insertQueue`)
			}
			if t.insertTriggered(args) { // if full then flush
				t.Flush()
			}
		case <-t.TriggerExit:
			if t.Debug {
				fmt.Println(`TriggerExit`)
			}
			if t.OnExitCallback != nil {
				t.OnExitCallback()
			}
			t.exitTriggered = true
			if t.DontWaitMoreInsertAfterClose {
				// dequeue remaining queue
				remaining := len(t.insertQueue)
				for z := 0; z < remaining; z++ {
					args := <-t.insertQueue
					if t.insertTriggered(args) {
						t.Flush()
					}
				}
				t.Flush()
				fmt.Println(`Flushed all insertQueue, exiting..`)
				return
			}
		case <-t.TriggerForceFlush:
			if t.Debug {
				fmt.Println(`TriggerForceFlush`)
			}
			t.Flush()
		case <-ticker.C:
			if t.Debug {
				fmt.Println(`TriggerTimer`)
			}
			t.Flush()
			if t.exitTriggered && len(t.insertQueue) == 0 {
				t.Flush()
				fmt.Println(`No more insert queue, exiting..`)
				return
			}
		}
	}
}

func (t *TimedBuffer) HandleTermSignal() {
	interrupt := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	//signal.Notify(interrupt, os.Interrupt, syscall.SIGKILL)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGHUP)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGINT)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGQUIT)

	<-interrupt
	fmt.Println(`caught signal`, interrupt)
	t.TriggerExit <- true
}
