package loadshedder

import "sync"

const shardCount = 64

// shardedMap is a concurrent map sharded by uint32 key to reduce lock
// contention. Each shard has its own RWMutex, so operations on different
// shards never contend. This replaces sync.Map which has higher overhead
// on the store path (internal mutex) and read path (two-level dirty/read
// map lookup).
type shardedMap[V any] struct {
	shards [shardCount]shard[V]
}

type shard[V any] struct {
	mu sync.RWMutex
	m  map[uint32]V
	_  [40]byte // pad to 64 bytes to avoid false sharing
}

func newShardedMap[V any]() *shardedMap[V] {
	var sm shardedMap[V]
	for i := range sm.shards {
		sm.shards[i].m = make(map[uint32]V)
	}
	return &sm
}

func (sm *shardedMap[V]) getShard(key uint32) *shard[V] {
	return &sm.shards[key%shardCount]
}

// Load returns the value for key and whether it was found.
func (sm *shardedMap[V]) Load(key uint32) (V, bool) {
	s := sm.getShard(key)
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	return v, ok
}

// LoadOrStore returns the existing value if present, otherwise stores and
// returns newValue. The loaded return value is true if the value was already
// present.
func (sm *shardedMap[V]) LoadOrStore(key uint32, newValue V) (V, bool) {
	s := sm.getShard(key)
	// Fast path: read lock
	s.mu.RLock()
	if v, ok := s.m[key]; ok {
		s.mu.RUnlock()
		return v, true
	}
	s.mu.RUnlock()

	// Slow path: write lock
	s.mu.Lock()
	if v, ok := s.m[key]; ok {
		s.mu.Unlock()
		return v, true
	}
	s.m[key] = newValue
	s.mu.Unlock()
	return newValue, false
}

// Delete removes the key from the map.
func (sm *shardedMap[V]) Delete(key uint32) {
	s := sm.getShard(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

// DeleteIf removes the key only if pred returns true for the current value.
// The predicate runs under the shard's write lock, ensuring no concurrent
// modification between the check and the delete.
func (sm *shardedMap[V]) DeleteIf(key uint32, pred func(V) bool) {
	s := sm.getShard(key)
	s.mu.Lock()
	if v, ok := s.m[key]; ok && pred(v) {
		delete(s.m, key)
	}
	s.mu.Unlock()
}

// Range calls f for each key-value pair. If f returns false, iteration stops.
func (sm *shardedMap[V]) Range(f func(key uint32, value V) bool) {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.RLock()
		for k, v := range s.m {
			if !f(k, v) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}
