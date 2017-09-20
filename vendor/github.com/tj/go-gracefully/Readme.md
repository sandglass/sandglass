
# go-gracefully

 Graceful shutdown utility with hard exit on second signal.

 View the [docs](http://godoc.org/github.com/tj/go-gracefully).

## Installation

```
$ go get github.com/tj/go-gracefully
```

## Example

  Typically something like:

```go
w.Start()
gracefully.Timeout = 10 * time.Second
gracefully.Shutdown()
w.Stop()
```

# License

 MIT