package _all_test

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
	"github.com/kokizzu/goproc"
	"github.com/kokizzu/gotro/I"
	"github.com/kokizzu/gotro/S"
	"github.com/stretchr/testify/assert"
)

func onDevMode() bool {
	return os.Getenv(`ENV`) == ``
}

func TestTimedBufferSingle(t *testing.T) {
	if !onDevMode() {
		return
	}
	daemon := goproc.New()

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../1single_test/single_main.go`},
	})

	daemon.Start(cmdId)

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Equal(t, 105, count)
}

func TestTimedBufferMulti(t *testing.T) {
	if !onDevMode() {
		return
	}

	daemon := goproc.New()

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../2multi_test/multi_main.go`},
	})

	daemon.Start(cmdId)

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Equal(t, 250, count)
}

// sigterm sent during insert (cannot be simulated normally using goproc/golang's exe.Process.Signal)
func TestTimedBufferSignal(t *testing.T) {
	if !onDevMode() {
		return
	}
	daemon := goproc.New()

	waitInsert15x := make(chan bool)

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../3signal_test/signal_main.go`},
		OnStdout: func(cmd *goproc.Cmd, line string) error {
			fmt.Println(line)
			if line == `Insert 14` {
				waitInsert15x <- true
			}
			return nil
		},
	})

	go daemon.Start(cmdId)
	<-waitInsert15x
	daemon.Signal(cmdId, syscall.SIGTERM)
	// goproc/exe.Process.Signal cannot forward signal properly so this immediately exiting, already tried:
	//daemon.Signal(cmdId,syscall.SIGKILL)
	//err := daemon.Terminate(cmdId)
	//L.IsError(err,`failed terminate program`)

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Greater(t, count, 8) // at least first commit should exists if terminated forcefully
	fmt.Println(`successful insert before killed: `, count)
}

func TestTimedBufferReal(t *testing.T) {
	if !onDevMode() {
		return
	}
	daemon := goproc.New()

	port := `:0`
	serverReady := make(chan bool)
	serverShutdown := make(chan bool)

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../4real_test/real_main.go`},
		OnStdout: func(cmd *goproc.Cmd, line string) error {
			//fmt.Println(line)
			if S.StartsWith(line, `Port:`) {
				port = S.RightOf(line, `:`)
			} else if line == `Ready` {
				serverReady <- true
			} else if S.StartsWith(line, `WaitFinalFlush done`) {
				serverShutdown <- true
			}
			return nil
		},
	})

	go daemon.Start(cmdId)

	<-serverReady
	time.Sleep(90 * time.Millisecond) // wait a moment for server to really listen

	// hit APIs in parallel
	wg := sync.WaitGroup{}
	const ParallelCount = 10
	const RecordCount = 45
	const ShiftCount = 1000 // make sure id not duplicate
	for z := 0; z < ParallelCount; z++ {
		wg.Add(1)
		go func(goroutineId int) {
			for z := 0; z < RecordCount; z++ {
				v := I.ToStr(goroutineId + z)
				//fmt.Println(v)
				_, err := http.Get(`http://localhost:` + port + `/ingest/` + v)
				L.IsError(err)
			}
			wg.Done()
		}(z * ShiftCount)
	}
	wg.Wait()

	_, err := http.Get(`http://localhost:` + port + `/exit`)
	L.IsError(err)

	// wait for server shutdown
	<-serverShutdown

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Equal(t, count, ParallelCount*RecordCount)
	fmt.Println(`successful insert before killed: `, count)
}

func TestTimedBufferImmediateExit(t *testing.T) {
	if !onDevMode() {
		return
	}

	daemon := goproc.New()

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../5wait_test/wait_main.go`},
	})

	daemon.Start(cmdId)

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Equal(t, 1250, count)
}

func TestTimedBufferNoinput(t *testing.T) {
	if !onDevMode() {
		return
	}

	daemon := goproc.New()

	workDir, _ := os.Getwd()
	cmdId := daemon.AddCommand(&goproc.Cmd{
		Program:    `/usr/bin/go`,
		Parameters: []string{`run`, workDir + `/../6noinput_test/noinput_main.go`},
	})

	daemon.Start(cmdId)

	conn := shared.ConnectClickhouse()
	count := shared.DummyCount(conn)
	assert.Equal(t, 0, count)
}
