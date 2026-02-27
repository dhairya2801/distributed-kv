package engine

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type WALEntry struct {
	Op    byte
	Key   []byte
	Value []byte
}

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mu     sync.Mutex
}

const (
	OpPut    = 0
	OpDelete = 1
)

func (w *WAL) ReadAll() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Read from offset 0 using a separate file descriptor so we don't
	// disturb the write position.
	file, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []WALEntry

	for {
		op, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var keyLen int32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}

		var valLen int32
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}

		value := make([]byte, valLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		entries = append(entries, WALEntry{
			Op:    op,
			Key:   key,
			Value: value,
		})
	}

	return entries, nil
}

func NewWAL(filename string) (*WAL, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end so new writes append.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriter(f),
	}, nil
}

func (w *WAL) Write(op byte, key []byte, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.WriteByte(op); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, int32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, int32(len(value))); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}

	return w.writer.Flush()
}

// Reset truncates the WAL file, discarding all entries. Called after a
// successful MemTable flush to SSTable. The WAL remains open for new writes.
func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Flush any buffered data first so we don't have stale bytes in the writer.
	w.writer.Reset(nil) // discard buffered data

	// Truncate and seek to beginning.
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Re-initialise the buffered writer on the truncated file.
	w.writer.Reset(w.file)
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	return w.file.Close()
}

// EntryCount returns the number of entries currently in the WAL.
func (w *WAL) EntryCount() int {
	entries, err := w.ReadAll()
	if err != nil {
		return 0
	}
	return len(entries)
}
