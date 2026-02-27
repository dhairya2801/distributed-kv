package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"distributed-kv/engine"

	"nhooyr.io/websocket"
)

// Handler holds the HTTP handlers for the visualizer.
type Handler struct {
	tree        *engine.LSMTree
	broadcaster *Broadcaster
	dataDir     string

	mu sync.Mutex // protects tree reset
}

// NewHandler creates a new Handler.
func NewHandler(tree *engine.LSMTree, bc *Broadcaster, dataDir string) *Handler {
	return &Handler{
		tree:        tree,
		broadcaster: bc,
		dataDir:     dataDir,
	}
}

// RegisterRoutes registers all HTTP routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/ws", h.handleWS)
	mux.HandleFunc("/api/put", h.handlePut)
	mux.HandleFunc("/api/delete", h.handleDelete)
	mux.HandleFunc("/api/get", h.handleGet)
	mux.HandleFunc("/api/bulk", h.handleBulk)
	mux.HandleFunc("/api/snapshot", h.handleSnapshot)
	mux.HandleFunc("/api/sstable", h.handleSSTable)
	mux.HandleFunc("/api/flush", h.handleFlush)
	mux.HandleFunc("/api/reset", h.handleReset)
}

func (h *Handler) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // allow any origin for local dev
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.broadcaster.AddClient(conn)
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	err := h.tree.Put([]byte(req.Key), []byte(req.Value))
	h.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	err := h.tree.Delete([]byte(req.Key))
	h.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	val, found, err := h.tree.Get([]byte(req.Key))
	h.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"key":   req.Key,
		"value": string(val),
		"found": found,
	})
}

func (h *Handler) handleBulk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Count  int    `json:"count"`
		Prefix string `json:"prefix"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Count <= 0 {
		req.Count = 100
	}
	if req.Prefix == "" {
		req.Prefix = "key"
	}
	h.mu.Lock()
	for i := 0; i < req.Count; i++ {
		key := fmt.Sprintf("%s%04d", req.Prefix, i)
		val := fmt.Sprintf("value%04d", i)
		if err := h.tree.Put([]byte(key), []byte(val)); err != nil {
			h.mu.Unlock()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	h.mu.Unlock()
	writeJSON(w, map[string]interface{}{"status": "ok", "inserted": req.Count})
}

func (h *Handler) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.mu.Lock()
	snap := h.tree.Snapshot()
	h.mu.Unlock()
	writeJSON(w, snap)
}

func (h *Handler) handleSSTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path parameter is required", http.StatusBadRequest)
		return
	}

	// Security: only allow files inside our data directory.
	absPath, err := filepath.Abs(path)
	if err != nil {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	absDir, _ := filepath.Abs(h.dataDir)
	rel, err := filepath.Rel(absDir, absPath)
	if err != nil || len(rel) > 1 && rel[:2] == ".." {
		http.Error(w, "path outside data directory", http.StatusForbidden)
		return
	}

	reader, err := engine.OpenSSTable(absPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	entries, err := reader.AllEntries()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type entryJSON struct {
		Key     string `json:"key"`
		Value   string `json:"value"`
		Deleted bool   `json:"deleted,omitempty"`
	}
	type indexJSON struct {
		Key    string `json:"key"`
		Offset int64  `json:"offset"`
	}

	jsonEntries := make([]entryJSON, len(entries))
	for i, e := range entries {
		jsonEntries[i] = entryJSON{Key: string(e.Key), Value: string(e.Value), Deleted: e.Deleted}
	}

	idx := reader.IndexEntries()
	jsonIdx := make([]indexJSON, len(idx))
	for i, ie := range idx {
		jsonIdx[i] = indexJSON{Key: string(ie.Key), Offset: ie.Offset}
	}

	writeJSON(w, map[string]interface{}{
		"path":     path,
		"dataSize": reader.DataSize(),
		"entries":  jsonEntries,
		"index":    jsonIdx,
	})
}

func (h *Handler) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.mu.Lock()
	err := h.tree.ForceFlush()
	h.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *Handler) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	h.tree.Close()

	// Remove all SSTable files in the data dir.
	// We keep the directory itself.
	entries, _ := filepath.Glob(filepath.Join(h.dataDir, "*.sst"))
	for _, e := range entries {
		_ = removeFile(e)
	}
	// Remove WAL.
	_ = removeFile(filepath.Join(h.dataDir, "wal.log"))

	tree, err := engine.OpenLSMTree(h.dataDir,
		engine.WithMemTableLimit(512),
		engine.WithL0CompactionThreshold(2),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tree.SetObserver(h.broadcaster)
	h.tree = tree
	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func removeFile(path string) error {
	return os.Remove(path)
}
