package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

const defaultReplicas = 150

// Ring is a consistent hash ring. It maps keys to nodes using virtual nodes
// for even distribution. All methods are goroutine-safe.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	ring     []uint64          // sorted virtual-node hashes
	nodes    map[uint64]string // hash -> real node ID
}

// New creates a Ring with replicas virtual nodes per real node.
// If replicas <= 0, defaultReplicas (150) is used.
func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = defaultReplicas
	}
	return &Ring{
		replicas: replicas,
		nodes:    make(map[uint64]string),
	}
}

func hashKey(s string) uint64 {
	h := sha256.Sum256([]byte(s))
	return binary.LittleEndian.Uint64(h[:8])
}

// AddNode adds a node to the ring. Adding an already-present node is a no-op.
func (r *Ring) AddNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", id, i))
		if _, exists := r.nodes[h]; !exists {
			r.ring = append(r.ring, h)
			r.nodes[h] = id
		}
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
}

// RemoveNode removes a node from the ring. Removing a non-existent node is a no-op.
func (r *Ring) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.replicas; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", id, i))
		delete(r.nodes, h)
	}

	// Rebuild the sorted slice, keeping only hashes still in the map.
	kept := r.ring[:0]
	for _, h := range r.ring {
		if _, ok := r.nodes[h]; ok {
			kept = append(kept, h)
		}
	}
	r.ring = kept
}

// GetNode returns the node responsible for key, or "" if the ring is empty.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return ""
	}

	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0 // wrap around
	}
	return r.nodes[r.ring[idx]]
}

// GetNodes returns up to n distinct nodes for key in preference order
// (primary first). Useful for replication. Returns fewer entries if the
// ring has fewer distinct nodes than n.
func (r *Ring) GetNodes(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 || n <= 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}

	seen := make(map[string]struct{})
	result := make([]string, 0, n)
	for i := 0; i < len(r.ring) && len(result) < n; i++ {
		pos := (idx + i) % len(r.ring)
		nodeID := r.nodes[r.ring[pos]]
		if _, ok := seen[nodeID]; !ok {
			seen[nodeID] = struct{}{}
			result = append(result, nodeID)
		}
	}
	return result
}

// Len returns the number of distinct real nodes in the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, id := range r.nodes {
		seen[id] = struct{}{}
	}
	return len(seen)
}
