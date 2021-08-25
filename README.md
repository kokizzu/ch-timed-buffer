
# CH-Timed-Buffer

Poller for inserting rows to Clickhouse, this would wait specific time to flush inserts or when buffer full. Need modification if used with other database product.

## How To Use

create a `PrepareFunc` = the prepared statement
```
prepareFunc := func(tx *sql.Tx) *sql.Stmt {
	stmt, err := tx.Prepare(`INSERT INTO dummy1(strCol,intCol,floatCol,dateCol,timeCol) VALUES(?,?,?,?,?)`)
	L.IsError(err, `failed prepare insert to dummy1`)
	return stmt
}
```


create timed buffer, one per prepared statement, requirement:
1. clickhouse connection
2. max buffer count, check record size so it won't use too much memory, eg. 100K x 200 byte row = will use around 2 x 20 MB (double buffer)
3. flush interval and shutdown delay
4. function that create prepared statement
```
tb := chBuffer.NewTimedBuffer(conn, capacity, 1*time.Second, prepareFunc)
```

if you want to exit immediately after all flushed and exit triggered, without this, might deadlock if:
1. using multiple channel that depend each other
2. calling `Close()` or sending `TriggerExit` twice
```
// if this value true, any code after <- WaitFinalFlush will not be called
//   including main's defer
tb.ForceExitOnSignal = true
```

change default behavior from waiting remaining insert to exit immediately after flush if `Close()` or `TriggerExit` called.
```
// if this false (default) = you want to exit by waiting last flush ticker / no more traffic
// if this true, there will be loss data when there's pending channel queue more than buffer length or insert after close triggered
tb.DontWaitMoreInsertAfterClose = true
```

add callback to gracefully close other goroutine (eg. webserver) if needed, see `*_test` directory for example.
```
tb.OnExitCallback = func() {}
```

enqueue the insert
```
tb.Insert(...) // can be on other goroutine, it's thread safe, if insert > capacity, it would block
```

trigger exit manually (if needed)
```
tb.Close() 
tb.TriggerExit <- true // alternate syntax
```

make main func doesn't exit until last record flushed, should be the last on main
```
<- tb.WaitFinalFlush 
```
