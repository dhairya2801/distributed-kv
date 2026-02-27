package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Command types for the KV store.
const (
	CmdPut    byte = 1
	CmdDelete byte = 2
)

// Command represents a KV operation to be replicated through Raft.
type Command struct {
	Op    byte
	Key   []byte
	Value []byte // empty for CmdDelete
}

// EncodeCommand serializes a Command into bytes.
// Format: [op:1][keyLen:4][key:keyLen][valLen:4][val:valLen]
func EncodeCommand(cmd Command) []byte {
	keyLen := len(cmd.Key)
	valLen := len(cmd.Value)
	buf := make([]byte, 1+4+keyLen+4+valLen)
	buf[0] = cmd.Op
	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	copy(buf[5:5+keyLen], cmd.Key)
	binary.BigEndian.PutUint32(buf[5+keyLen:9+keyLen], uint32(valLen))
	copy(buf[9+keyLen:], cmd.Value)
	return buf
}

// DecodeCommand deserializes bytes into a Command.
func DecodeCommand(data []byte) (Command, error) {
	if len(data) < 9 { // 1 + 4 + 0 + 4 + 0 minimum
		return Command{}, errors.New("command: data too short")
	}
	op := data[0]
	if op != CmdPut && op != CmdDelete {
		return Command{}, fmt.Errorf("command: unknown op %d", op)
	}
	keyLen := int(binary.BigEndian.Uint32(data[1:5]))
	if len(data) < 5+keyLen+4 {
		return Command{}, errors.New("command: truncated key")
	}
	key := make([]byte, keyLen)
	copy(key, data[5:5+keyLen])

	valLen := int(binary.BigEndian.Uint32(data[5+keyLen : 9+keyLen]))
	if len(data) < 9+keyLen+valLen {
		return Command{}, errors.New("command: truncated value")
	}
	value := make([]byte, valLen)
	copy(value, data[9+keyLen:9+keyLen+valLen])

	return Command{Op: op, Key: key, Value: value}, nil
}
