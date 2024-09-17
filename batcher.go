package batcher

import (
	"errors"
	"sync"
	"time"
)

// Batcher is a structure that collects items and flushes buffer
// either when the buffer fills up or when timeout is reached.
// See examples/batcher/batcher.go for usage example.
type Batcher[T any] struct {
	ticker   *time.Ticker
	capacity int

	buffer []T
	output chan []T

	mu     sync.Mutex
	closed bool
}

// New creates a new instance of Batcher.
func New[T any](d time.Duration, capacity int) *Batcher[T] {
	if capacity <= 0 {
		panic("capacity must be greater than 0")
	}

	obj := &Batcher[T]{}

	obj.ticker = time.NewTicker(d)
	obj.capacity = capacity

	obj.buffer = make([]T, 0, obj.capacity)
	obj.output = make(chan []T, 10) // number of batches to keep

	go func() {
		for range obj.ticker.C {
			obj.mu.Lock()
			// the object may be closed while we were waiting for the lock
			// it is safe to exit, the last batch has been already processed
			if obj.closed {
				obj.mu.Unlock()
				return
			}

			obj.makeBatch()
			obj.mu.Unlock()
		}
	}()

	return obj
}

// Add adds an item to the batcher.
func (b *Batcher[T]) Add(item T) {
	err := b.AddE(item)
	if err != nil {
		panic(err)
	}
}

// AddE adds an item to the batcher.
// It returns an error if the batcher is closed.
func (b *Batcher[T]) AddE(item T) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("batcher is closed")
	}

	b.buffer = append(b.buffer, item)
	if len(b.buffer) >= b.capacity {
		b.makeBatch()
	}

	return nil
}

// C returns a channel that will receive batches.
func (b *Batcher[T]) C() <-chan []T {
	return b.output
}

// Close closes the batcher.
func (b *Batcher[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.ticker.Stop()
	b.closed = true

	b.makeBatch()
	close(b.output)
}

func (b *Batcher[T]) makeBatch() {
	if len(b.buffer) == 0 {
		return
	}
	b.output <- b.buffer
	b.buffer = make([]T, 0, b.capacity)
}
