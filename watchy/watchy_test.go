package watchy

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

func TestWatchy(t *testing.T) {
	w := New()
	w.Emit("test", nil)

	select {
	case <-w.Once("test"):
		require.Fail(t, "should not receive anytime")
	default:
	}

	ch := w.Once("test")
	w.Emit("test", nil)
	select {
	case <-ch:
	default:
		require.Fail(t, "should receive an event")
	}

	select {
	case <-w.Once("test"):
		require.Fail(t, "should not receive anytime")
	default:
	}
}

func TestConcurrent(t *testing.T) {
	w := New()

	want := 10
	// got := 0
	ctx, cancel := context.WithTimeout(context.TODO(), 4*time.Second)
	defer cancel()
	group, _ := errgroup.WithContext(ctx)
	var wg sync.WaitGroup
	wg.Add(want)
	for i := 0; i < want; i++ {
		i := i
		ch := w.Once("test")
		group.Go(func() error {
			fmt.Println("listener", i)
			<-ch
			return nil
		})
	}

	group.Go(func() error {
		w.Emit("test", nil)
		return nil
	})
	err := group.Wait()
	require.NoError(t, err)
}
