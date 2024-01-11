# Batcher

Batcher is a go library that allows to collect any items into a buffer and
flush the buffer either when the buffer fills up or when timeout is reached.

# Installation
```bash
go get github.com/pushwoosh/batcher
```

# Example
```go
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
```
