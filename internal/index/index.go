																																																																										// Package index provides in-memory index structures for the Raft state machine.
package index

import (
  	"sort"
  	"sync"
  )

type KeyValue struct {
  	Key   string
  	Value []byte
  }

type BTreeIndex struct {
  	mu      sync.RWMutex
  	entries []KeyValue
  }

func NewBTreeIndex() *BTreeIndex { return &BTreeIndex{} }

func (b *BTreeIndex) Put(key string, value []byte) {
  	b.mu.Lock()
  	defer b.mu.Unlock()
  	i := sort.Search(len(b.entries), func(i int) bool { return b.entries[i].Key >= key })
  	if i < len(b.entries) && b.entries[i].Key == key {
      		b.entries[i].Value = value
      		return
      	}
  	b.entries = append(b.entries, KeyValue{})
  	copy(b.entries[i+1:], b.entries[i:])
  	b.entries[i] = KeyValue{Key: key, Value: value}
  }

func (b *BTreeIndex) Get(key string) ([]byte, bool) {
  	b.mu.RLock()
  	defer b.mu.RUnlock()
  	i := sort.Search(len(b.entries), func(i int) bool { return b.entries[i].Key >= key })
  	if i < len(b.entries) && b.entries[i].Key == key {
      		return b.entries[i].Value, true
      	}
  	return nil, false
  }

func (b *BTreeIndex) Delete(key string) {
  	b.mu.Lock()
  	defer b.mu.Unlock()
  	i := sort.Search(len(b.entries), func(i int) bool { return b.entries[i].Key >= key })
  	if i < len(b.entries) && b.entries[i].Key == key {
      		b.entries = append(b.entries[:i], b.entries[i+1:]...)
      	}
  }

func (b *BTreeIndex) Range(lo, hi string) []KeyValue {
  	b.mu.RLock()
  	defer b.mu.RUnlock()
  	start := sort.Search(len(b.entries), func(i int) bool { return b.entries[i].Key >= lo })
  	var result []KeyValue
  	for i := start; i < len(b.entries) && b.entries[i].Key <= hi; i++ {
      		result = append(result, b.entries[i])
      	}
  	return result
  }

func (b *BTreeIndex) Len() int {
  	b.mu.RLock()
  	defer b.mu.RUnlock()
  	return len(b.entries)
  }

type HashIndex struct {
  	mu   sync.RWMutex
  	data map[string][]byte
  }

func NewHashIndex() *HashIndex { return &HashIndex{data: make(map[string][]byte)} }

func (h *HashIndex) Put(key string, value []byte) {
  	h.mu.Lock()
  	defer h.mu.Unlock()
  	h.data[key] = value
  }

func (h *HashIndex) Get(key string) ([]byte, bool) {
  	h.mu.RLock()
  	defer h.mu.RUnlock()
  	v, ok := h.data[key]
  	return v, ok
  }

func (h *HashIndex) Delete(key string) {
  	h.mu.Lock()
  	defer h.mu.Unlock()
  	delete(h.data, key)
  }

func (h *HashIndex) Len() int {
  	h.mu.RLock()
  	defer h.mu.RUnlock()
  	return len(h.data)
  }
