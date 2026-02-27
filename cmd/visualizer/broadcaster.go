package main

import (
	"context"
	"encoding/json"
	"sync"

	"distributed-kv/engine"

	"nhooyr.io/websocket"
)

// client represents a single WebSocket connection.
type client struct {
	conn *websocket.Conn
	ch   chan []byte
}

// Broadcaster implements engine.Observer and fans out events to all
// connected WebSocket clients.
type Broadcaster struct {
	mu      sync.Mutex
	clients map[*client]struct{}
}

// NewBroadcaster creates a new Broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		clients: make(map[*client]struct{}),
	}
}

// eventNames maps EventType to a human-readable string for JSON.
var eventNames = map[engine.EventType]string{
	engine.EventWALWrite:        "wal_write",
	engine.EventMemTableInsert:  "memtable_insert",
	engine.EventMemTableDelete:  "memtable_delete",
	engine.EventFlushBegin:      "flush_begin",
	engine.EventFlushComplete:   "flush_complete",
	engine.EventCompactBegin:    "compact_begin",
	engine.EventCompactComplete: "compact_complete",
	engine.EventGetBegin:        "get_begin",
	engine.EventGetMemTable:     "get_memtable",
	engine.EventGetImm:          "get_imm",
	engine.EventGetL0:           "get_l0",
	engine.EventGetL1:           "get_l1",
	engine.EventGetResult:       "get_result",
	engine.EventWALReset:        "wal_reset",
}

// jsonEvent is the JSON-serialised form of an engine.Event.
type jsonEvent struct {
	Type       string       `json:"type"`
	Key        string       `json:"key,omitempty"`
	Value      string       `json:"value,omitempty"`
	Deleted    bool         `json:"deleted,omitempty"`
	Found      bool         `json:"found,omitempty"`
	SSTPath    string       `json:"sstPath,omitempty"`
	Level      int          `json:"level,omitempty"`
	EntryIdx   int          `json:"entryIdx,omitempty"`
	EntryTotal int          `json:"entryTotal,omitempty"`
	IndexPos   int          `json:"indexPos,omitempty"`
	Stats      engine.Stats `json:"stats"`
}

// OnEvent implements engine.Observer. Must be non-blocking.
func (b *Broadcaster) OnEvent(e engine.Event) {
	je := jsonEvent{
		Type:       eventNames[e.Type],
		Key:        string(e.Key),
		Value:      string(e.Value),
		Deleted:    e.Deleted,
		Found:      e.Found,
		SSTPath:    e.SSTPath,
		Level:      e.Level,
		EntryIdx:   e.EntryIdx,
		EntryTotal: e.EntryTotal,
		IndexPos:   e.IndexPos,
		Stats:      e.Stats,
	}
	data, err := json.Marshal(je)
	if err != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	for c := range b.clients {
		select {
		case c.ch <- data:
		default:
			// Drop if the client's buffer is full.
		}
	}
}

// AddClient registers a new WebSocket connection and starts a writer goroutine.
func (b *Broadcaster) AddClient(conn *websocket.Conn) {
	c := &client{
		conn: conn,
		ch:   make(chan []byte, 256),
	}

	b.mu.Lock()
	b.clients[c] = struct{}{}
	b.mu.Unlock()

	// Writer goroutine: drains the channel and writes to the WebSocket.
	go func() {
		defer func() {
			b.mu.Lock()
			delete(b.clients, c)
			b.mu.Unlock()
			conn.Close(websocket.StatusNormalClosure, "")
		}()

		for msg := range c.ch {
			err := conn.Write(context.Background(), websocket.MessageText, msg)
			if err != nil {
				return
			}
		}
	}()

	// Reader goroutine: keeps reading to detect client disconnection.
	go func() {
		for {
			_, _, err := conn.Read(context.Background())
			if err != nil {
				close(c.ch)
				return
			}
		}
	}()
}
