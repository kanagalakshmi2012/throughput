package main
import (
	"fmt"
	"net/http"
	"sync"
)
type KVStore struct {
	data map[string]string
	mu   sync.RWMutex
}
var cluster = []KVStore{
	{data: make(map[string]string)},
	{data: make(map[string]string)},
	{data: make(map[string]string)},
}

func writeKey(key, value string) {
	for i := range cluster {
		cluster[i].mu.Lock()
		cluster[i].data[key] = value
		cluster[i].mu.Unlock()
	}
	fmt.Printf("Written '%s':'%s' to all nodes\n", key, value)
}
func readKey(key string) (string, bool) {
	cluster[0].mu.RLock()
	defer cluster[0].mu.RUnlock()
	val, exists := cluster[0].data[key]
	return val, exists
}
func writeHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	if key == "" || value == "" {
		http.Error(w, "Missing key or value", http.StatusBadRequest)
		return
	}
	writeKey(key, value)
	w.Write([]byte("OK"))
}
func readHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}
	value, exists := readKey(key)
	if !exists {
		http.NotFound(w, r)
		return
	}

	w.Write([]byte(fmt.Sprintf("%s", value)))
}
func main() {
	http.HandleFunc("/write", writeHandler)
	http.HandleFunc("/read", readHandler)
}
