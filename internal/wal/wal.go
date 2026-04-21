// Package wal implements a Write-Ahead Log for durable Raft state persistence.
// Each record is length-prefixed and CRC32-checksummed before being written to disk.
package wal

import (
  	"bufio"
  	"encoding/binary"
  	"encoding/json"
  	"hash/crc32"
  	"io"
  	"os"
  	"path/filepath"
  	"sync"

  	"go.uber.org/zap"

  	"github.com/pierre21carrion-hash/raft-consensus/internal/raft"
  )

const walFileName = "raft.wal"

// record types
const (
  	recordState uint8 = iota + 1
  	recordEntry
  )

type stateRecord struct {
  	Term     raft.Term   `json:"term"`
  	VotedFor raft.NodeID `json:"voted_for"`
  }

// WAL is a write-ahead log for durable Raft state
type WAL struct {
  	mu   sync.Mutex
  	path string
  	f    *os.File
  	w    *bufio.Writer
  }

// NewWAL opens (or creates) the WAL at dir/raft.wal
func NewWAL(dir string) (*WAL, error) {
  	if err := os.MkdirAll(dir, 0755); err != nil {
      		return nil, err
      	}
  	path := filepath.Join(dir, walFileName)
  	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
  	if err != nil {
      		return nil, err
      	}
  	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
  }

// SaveState persists the current term and votedFor atomically
func (w *WAL) SaveState(term raft.Term, votedFor raft.NodeID) {
  	w.mu.Lock()
  	defer w.mu.Unlock()
  	rec := stateRecord{Term: term, VotedFor: votedFor}
  	data, _ := json.Marshal(rec)
  	w.writeRecord(recordState, data)
  }

// AppendEntry appends a log entry to the WAL
func (w *WAL) AppendEntry(e raft.Entry) {
  	w.mu.Lock()
  	defer w.mu.Unlock()
  	data, _ := json.Marshal(e)
  	w.writeRecord(recordEntry, data)
  }

// LoadState reads back the latest term, votedFor, and all log entries from disk
func (w *WAL) LoadState() (raft.Term, raft.NodeID, []raft.Entry) {
  	w.mu.Lock()
  	defer w.mu.Unlock()

  	f, err := os.Open(w.path)
  	if err != nil {
      		return 0, "", nil
      	}
  	defer f.Close()

  	var term raft.Term
  	var votedFor raft.NodeID
  	var entries []raft.Entry
  	r := bufio.NewReader(f)

  	for {
      		recType, data, err := readRecord(r)
      		if err == io.EOF {
            			break
            		}
      		if err != nil {
            			zap.L().Warn("wal: corrupt record, stopping replay", zap.Error(err))
            			break
            		}
      		switch recType {
            		case recordState:
            			var s stateRecord
            			if json.Unmarshal(data, &s) == nil {
                    				term = s.Term
                    				votedFor = s.VotedFor
                    			}
            		case recordEntry:
            			var e raft.Entry
            			if json.Unmarshal(data, &e) == nil {
                    				entries = append(entries, e)
                    			}
            		}
      	}
  	return term, votedFor, entries
  }

// writeRecord writes: [type:1][crc:4][len:4][data:len]
func (w *WAL) writeRecord(recType uint8, data []byte) {
  	crc := crc32.ChecksumIEEE(data)
  	buf := make([]byte, 9+len(data))
  	buf[0] = recType
  	binary.BigEndian.PutUint32(buf[1:5], crc)
  	binary.BigEndian.PutUint32(buf[5:9], uint32(len(data)))
  	copy(buf[9:], data)
  	_, _ = w.w.Write(buf)
  	_ = w.w.Flush()
  	_ = w.f.Sync()
  }

func readRecord(r *bufio.Reader) (uint8, []byte, error) {
  	header := make([]byte, 9)
  	if _, err := io.ReadFull(r, header); err != nil {
      		return 0, nil, err
      	}
  	recType := header[0]
  	expectedCRC := binary.BigEndian.Uint32(header[1:5])
  	length := binary.BigEndian.Uint32(header[5:9])
  	data := make([]byte, length)
  	if _, err := io.ReadFull(r, data); err != nil {
      		return 0, nil, err
      	}
  	if crc32.ChecksumIEEE(data) != expectedCRC {
      		return 0, nil, io.ErrUnexpectedEOF
      	}
  	return recType, data, nil
  }
