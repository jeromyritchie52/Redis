package main

import (
	"strconv"
	"testing"
	"time"
)

func TestActiveExpireCycle_TimeLimit(t *testing.T) {
	server := NewServer()
	server.SetActiveExpireEffort(1) // low effort -> low time limit (250us)

	// Populate database with expired keys
	db := server.dbs[0]
	db.mu.Lock()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 100000; i++ {
		key := "key:" + strconv.Itoa(i)
		db.dict[key] = "val"
		db.expires[key] = now - 1000 // expired 1s ago
	}
	db.mu.Unlock()

	// Run activeExpireCycle and measure time
	start := time.Now()
	server.activeExpireCycle()
	elapsed := time.Since(start)

	// The time limit for effort 1 is 250us. It should be very small.
	if elapsed > 15*time.Millisecond {
		t.Errorf("activeExpireCycle took too long: %v", elapsed)
	}

	// Verify that not all keys were expired (since it should have timed out)
	db.mu.RLock()
	remaining := len(db.dict)
	db.mu.RUnlock()
	if remaining == 0 {
		t.Errorf("Expected some keys to remain due to time limit, but all were expired")
	}
}

func TestActiveExpireCycle_StatePreservation(t *testing.T) {
	server := NewServer()
	server.SetActiveExpireEffort(1)

	// Put expired keys in DB 0 and DB 1
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for dbIdx := 0; dbIdx < 2; dbIdx++ {
		db := server.dbs[dbIdx]
		db.mu.Lock()
		for i := 0; i < 50000; i++ {
			key := "key:" + strconv.Itoa(i)
			db.dict[key] = "val"
			db.expires[key] = now - 1000
		}
		db.mu.Unlock()
	}

	// Run cycle once
	server.activeExpireCycle()
	firstDbIdx := server.lastActiveExpireDbIndex

	// Run cycle again
	server.activeExpireCycle()
	secondDbIdx := server.lastActiveExpireDbIndex

	// The index should have changed/incremented
	if firstDbIdx == secondDbIdx && firstDbIdx == 0 {
		t.Errorf("Expected database index to advance, but it stayed at %d", firstDbIdx)
	}
}

func TestActiveExpireCycle_AdaptiveSampling(t *testing.T) {
	server := NewServer()
	server.SetActiveExpireEffort(4)

	// Populate DB 0 with 95% non-expired keys and 5% expired keys
	db := server.dbs[0]
	db.mu.Lock()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 100; i++ {
		key := "key:" + strconv.Itoa(i)
		db.dict[key] = "val"
		if i < 5 { // 5% expired
			db.expires[key] = now - 1000
		} else { // 95% not expired
			db.expires[key] = now + 100000
		}
	}
	db.mu.Unlock()

	server.activeExpireCycle()

	// Since the expired percentage is low (< 10%), it should terminate early.
	// Let's verify that the non-expired keys are still there.
	db.mu.RLock()
	remaining := len(db.dict)
	db.mu.RUnlock()

	if remaining < 95 {
		t.Errorf("Expected at least 95 keys to remain, got %d", remaining)
	}
}