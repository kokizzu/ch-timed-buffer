
# CH-Timed-Buffer

Poller for inserting rows to Clickhouse, this would wait specific time to flush inserts or when buffer full. Need modification if used with other database product.

## How To Use

create a `PrepareFunc` = the prepared statement
```go
prepareFunc := func(tx *sql.Tx) *sql.Stmt {
	stmt, err := tx.Prepare(`INSERT INTO dummy1(strCol,intCol,floatCol,dateCol,timeCol) VALUES(?,?,?,?,?)`)
	L.IsError(err, `failed prepare insert to dummy1`)
	return stmt
}
```


create timed buffer, one per prepared statement, requirement:
1. clickhouse connection
2. max buffer count (capacity), measure record size so it won't use too much memory, eg. 100K x 200 byte row = will use around 2 x 20 MB (double buffer), clickhouse can ingest 600K-1 million records per second, don't set it too low (<10K) or it would throw  an error like this `code: 252, message: Too many parts (300). Merges are processing significantly slower than inserts`
3. flush interval and shutdown delay
4. function that create prepared statement
```go
tb := chBuffer.NewTimedBuffer(conn, capacity, 1*time.Second, prepareFunc)
```

if you want to exit immediately after all flushed and exit triggered, without this, might deadlock if:
1. using multiple channel that depend each other
2. calling `Close()` or sending `TriggerExit` twice
```go
// if this value true, any code after <- WaitFinalFlush will not be called
//   including main's defer
tb.ForceExitOnSignal = true
```

change default behavior from waiting remaining insert to exit immediately after flush if `Close()` or `TriggerExit` called.
```go
// if this false (default) = you want to exit by waiting last flush ticker / no more traffic
// if this true, there will be loss data when there's pending channel queue more than buffer length or insert after close triggered
tb.DontWaitMoreInsertAfterClose = true
```

add callback to gracefully close other goroutine (eg. webserver) if needed, see `*_test` directory for example.
```go
tb.OnExitCallback = func() {}
```

ignore interrupt signal
```go
tb.IgnoreInterrupt = true
```

enqueue the insert
```go
tb.Insert(...) // can be on other goroutine, it's thread safe, if insert > capacity, it would block
```

trigger exit manually (if needed)
```go
tb.Close() 
tb.TriggerExit <- true // alternate syntax
```

make main func doesn't exit until last record flushed, should be the last on main
```go
<- tb.WaitFinalFlush 
```

## Run tests

`make test`

![image](https://user-images.githubusercontent.com/1061610/186598659-f03d2a8a-c9ce-4d8d-bd10-110d73511807.png)

