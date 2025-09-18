package batcher

import (
	"sync"
	"testing"
	"time"
)

func TestBatcherRaceCondition(t *testing.T) {
	// Test for the race condition where timer and capacity-based batching
	// can create inconsistent batch sizes
	capacity := 10
	timeout := 50 * time.Millisecond

	b := New[int](timeout, capacity)
	defer b.Close()

	var batches [][]int
	var mu sync.Mutex

	// Collect all batches
	go func() {
		for batch := range b.C() {
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
		}
	}()

	// Add items in bursts that could trigger the race condition
	// This simulates high-frequency adds that reach capacity
	// while timer is also trying to flush
	for burst := 0; burst < 5; burst++ {
		// Add exactly capacity items quickly
		for i := 0; i < capacity; i++ {
			b.Add(i)
		}
		// Small delay to let timer potentially interfere
		time.Sleep(5 * time.Millisecond)
	}

	// Wait a bit for any pending timer flushes
	time.Sleep(timeout * 2)
	b.Close()

	// Verify results
	mu.Lock()
	defer mu.Unlock()

	if len(batches) == 0 {
		t.Fatal("Expected at least one batch")
	}

	// Count items and verify no duplicates/losses
	totalItems := 0
	capacityBasedBatches := 0

	for i, batch := range batches {
		t.Logf("Batch %d: size %d", i, len(batch))
		totalItems += len(batch)

		// Most batches should be at capacity (from our test pattern)
		if len(batch) == capacity {
			capacityBasedBatches++
		}

		// No batch should be empty (this was already handled)
		if len(batch) == 0 {
			t.Error("Found empty batch")
		}
	}

	// We added 5 bursts of 10 items = 50 total
	expectedItems := 5 * capacity
	if totalItems != expectedItems {
		t.Errorf("Expected %d items total, got %d", expectedItems, totalItems)
	}

	// Most batches should be capacity-based since we're adding in exact multiples
	if capacityBasedBatches < 3 {
		t.Errorf("Expected at least 3 capacity-based batches, got %d", capacityBasedBatches)
	}
}

func TestBatcherTimerReset(t *testing.T) {
	// Test that timer resets properly when capacity is reached
	capacity := 5
	shortTimeout := 20 * time.Millisecond

	b := New[int](shortTimeout, capacity)
	defer b.Close()

	var batches [][]int
	var timestamps []time.Time
	start := time.Now()

	// Collect batches with timestamps
	go func() {
		for batch := range b.C() {
			batches = append(batches, batch)
			timestamps = append(timestamps, time.Now())
		}
	}()

	// Add exactly capacity items (should trigger immediate batch)
	for i := 0; i < capacity; i++ {
		b.Add(i)
	}

	// Wait for the batch to be processed
	time.Sleep(5 * time.Millisecond)

	// Add one more item and wait for timer
	b.Add(capacity)

	// Wait longer than timeout to ensure timer fires
	time.Sleep(shortTimeout * 2)
	b.Close()

	// Verify we got expected batches
	if len(batches) != 2 {
		t.Fatalf("Expected 2 batches, got %d", len(batches))
	}

	// First batch should be at capacity
	if len(batches[0]) != capacity {
		t.Errorf("First batch should have %d items, got %d", capacity, len(batches[0]))
	}

	// Second batch should have 1 item (from timer)
	if len(batches[1]) != 1 {
		t.Errorf("Second batch should have 1 item, got %d", len(batches[1]))
	}

	// First batch should be almost immediate (capacity-based)
	firstBatchTime := timestamps[0].Sub(start)
	if firstBatchTime > 10*time.Millisecond {
		t.Errorf("First batch took too long: %v", firstBatchTime)
	}

	// Second batch should come after timeout (timer-based)
	secondBatchTime := timestamps[1].Sub(timestamps[0])
	if secondBatchTime < shortTimeout {
		t.Errorf("Second batch came too early: %v (expected >= %v)", secondBatchTime, shortTimeout)
	}
}

func TestBatcherConcurrency(t *testing.T) {
	// Test high concurrency to expose race conditions
	capacity := 100
	timeout := 10 * time.Millisecond
	numGoroutines := 10
	itemsPerGoroutine := 1000

	b := New[int](timeout, capacity)
	defer b.Close()

	var batches [][]int
	var mu sync.Mutex

	// Collect batches
	go func() {
		for batch := range b.C() {
			mu.Lock()
			batches = append(batches, batch)
			mu.Unlock()
		}
	}()

	// Start multiple goroutines adding items concurrently
	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < itemsPerGoroutine; i++ {
				b.Add(goroutineID*itemsPerGoroutine + i)
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(timeout * 2) // Let final timer-based batch flush
	b.Close()

	// Verify all items were processed exactly once
	mu.Lock()
	defer mu.Unlock()

	totalItems := 0
	for _, batch := range batches {
		totalItems += len(batch)

		// No empty batches
		if len(batch) == 0 {
			t.Error("Found empty batch in concurrent test")
		}
	}

	expectedItems := numGoroutines * itemsPerGoroutine
	if totalItems != expectedItems {
		t.Errorf("Expected %d items total, got %d", expectedItems, totalItems)
	}

	t.Logf("Processed %d items in %d batches", totalItems, len(batches))
}