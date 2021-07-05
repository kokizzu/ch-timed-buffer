package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/kokizzu/ch-timed-buffer"
	shared "github.com/kokizzu/ch-timed-buffer/0shared_test"
	"github.com/kokizzu/gotro/I"
	"github.com/kokizzu/gotro/S"
)

// test multiple goroutine running insert using webserver
// the webserver need to be gracefully shutted down (so it won't receive more insert)
// kubernetes will stop traffic to the pod before sending sigterm to pod's main process
func main() {
	start := time.Now()
	conn := shared.ConnectClickhouse()

	shared.InitTableAndTruncate(conn)

	tb := ch_timed_buffer.NewTimedBuffer(conn, 10, 1*time.Second, shared.PrepareFunc)

	listener, err := net.Listen("tcp", ":0")
	L.IsError(err, `failed listen, all available port used?`)
	fmt.Println(`Port:` + I.ToStr(listener.Addr().(*net.TCPAddr).Port))

	tb.OnExitCallback = func() {
		listener.Close()
	}

	router := httprouter.New()
	router.GET("/ingest/:v", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		v := S.ToInt(p.ByName(`v`))
		tb.Insert(shared.InsertValues(&start, v))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	router.GET(`/exit`, func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		tb.TriggerExit <- true
	})

	fmt.Println(`Ready`)
	log.Println(http.Serve(listener, router))

	<-tb.WaitFinalFlush
}
