package engine

// EventType identifies the kind of LSM operation being observed.
type EventType int

const (
	EventWALWrite EventType = iota
	EventMemTableInsert
	EventMemTableDelete
	EventFlushBegin    // memTable rotated to imm
	EventFlushComplete // new L0 SSTable created
	EventCompactBegin
	EventCompactComplete
	EventGetBegin
	EventGetMemTable // searched active memtable (found or not)
	EventGetImm      // searched immutable memtable
	EventGetL0       // searched L0 SSTable (includes index position)
	EventGetL1       // searched L1 SSTable
	EventGetResult   // final answer
	EventWALReset
)

// Event carries details about a single LSM operation for visualization.
type Event struct {
	Type       EventType `json:"type"`
	Key        []byte    `json:"key,omitempty"`
	Value      []byte    `json:"value,omitempty"`
	Deleted    bool      `json:"deleted,omitempty"`
	Found      bool      `json:"found,omitempty"`
	SSTPath    string    `json:"sstPath,omitempty"`
	Level      int       `json:"level,omitempty"`
	EntryIdx   int       `json:"entryIdx,omitempty"`   // progress counter
	EntryTotal int       `json:"entryTotal,omitempty"` // total in operation
	IndexPos   int       `json:"indexPos,omitempty"`   // sparse index position used
	Stats      Stats     `json:"stats"`
}

// Stats is a snapshot of LSM tree metrics included with every event.
type Stats struct {
	MemTableSize     int64 `json:"memTableSize"`
	MemTableCapacity int64 `json:"memTableCapacity"`
	MemTableEntries  int   `json:"memTableEntries"`
	ImmEntries       int   `json:"immEntries"`
	L0Count          int   `json:"l0Count"`
	L0Threshold      int   `json:"l0Threshold"`
	L1Count          int   `json:"l1Count"`
	WALEntries       int   `json:"walEntries"`
}

// Observer receives events from the LSM tree. Implementations must be
// non-blocking — OnEvent is called while holding internal locks.
type Observer interface {
	OnEvent(Event)
}
