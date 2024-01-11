package main

import (
	"fmt"
	"time"

	"github.com/pushwoosh/batcher"
)

func main() {
	b := batcher.New[int](time.Millisecond*5, 20000)

	go func() {
		for i := 0; i < 1234567; i++ {
			b.Add(i)
		}
		b.Close()
	}()

	for batch := range b.C() {
		fmt.Println("batch size:", len(batch))
	}
}
