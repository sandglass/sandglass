package gracefully

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	Timeout = time.Minute
	Signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}
)

// Shutdown gracefully, or force exit after two consecutive signals.
func Shutdown() {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, Signals...)

	log.Printf("received signal %s", <-ch)
	log.Printf("terminating in %s", Timeout)

	go func() {
		select {
		case <-time.After(Timeout):
			log.Fatalf("timeout reached: terminating")
		case s := <-ch:
			log.Fatalf("received signal %s: terminating", s)
		}
	}()
}
