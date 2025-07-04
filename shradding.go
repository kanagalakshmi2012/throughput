package main

import (
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"sync"
)

type Shard struct {
	data map[string]string
	mu   sync.RWMutex
}

type ShardedStore struct {
	shards []Shard
	count  int
}

func NewShardedStore(n int) *ShardedStore {
	shards := make([]Shard, n)
	for i := range shards {
		shards[i] = Shard{data: make(map[string]string)}
	}
	return &ShardedStore{shards: shards, count: n}
}

func (s *ShardedStore) getShard(key string) *Shard {
	h := fnv.New32a()
	h.Write([]byte(key))
	index := int(h.Sum32()) % s.count
	return &s.shards[index]
}

func (s *ShardedStore) Set(key, value string) {
	shard := s.getShard(key)
	shard.mu.Lock()
	shard.data[key] = value
	shard.mu.Unlock()
}

func (s *ShardedStore) Get(key string) (string, bool) {
	shard := s.getShard(key)
	shard.mu.RLock()
	val, ok := shard.data[key]
	shard.mu.RUnlock()
	return val, ok
}

var store = NewShardedStore(4)

func writeHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val := r.URL.Query().Get("value")
	if key == "" || val == "" {
		http.Error(w, "Missing key or value", http.StatusBadRequest)
		return
	}
	store.Set(key, val)
	w.Write([]byte("OK"))
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}
	val, ok := store.Get(key)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Write([]byte(val))
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	for i, shard := range store.shards {
		shard.mu.RLock()
		count := strconv.Itoa(len(shard.data))
		shard.mu.RUnlock()
		fmt.Fprintf(w, "Shard %d: %s keys\n", i, count)
	}
}
