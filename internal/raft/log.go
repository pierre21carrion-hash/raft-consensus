package raft

import (
  	"errors"
  	"sync"
  )

var ErrCompacted = errors.New("log entry has been compacted")

type RaftLog struct {
  	mu        sync.RWMutex
  	entries   []Entry
  	snapIndex LogIndex
  	snapTerm  Term
  }

func NewRaftLog() *RaftLog {
  	return &RaftLog{
      		entries:   []Entry{{Index: 0, Term: 0}},
      		snapIndex: 0,
      		snapTerm:  0,
      	}
  }

func (l *RaftLog) LastIndex() LogIndex {
  	l.mu.RLock()
  	defer l.mu.RUnlock()
  	return l.lastIndex()
  }

func (l *RaftLog) lastIndex() LogIndex {
  	n := len(l.entries)
  	if n == 0 {
      		return l.snapIndex
      	}
  	return l.entries[n-1].Index
  }

func (l *RaftLog) LastTerm() Term {
  	l.mu.RLock()
  	defer l.mu.RUnlock()
  	n := len(l.entries)
  	if n == 0 {
      		return l.snapTerm
      	}
  	return l.entries[n-1].Term
  }

func (l *RaftLog) Term(index LogIndex) (Term, error) {
  	l.mu.RLock()
  	defer l.mu.RUnlock()
  	if index < l.snapIndex {
      		return 0, ErrCompacted
      	}
  	if index == l.snapIndex {
      		return l.snapTerm, nil
      	}
  	offset := int(index-l.snapIndex) - 1
  	if offset < 0 || offset >= len(l.entries) {
      		return 0, errors.New("index out of range")
      	}
  	return l.entries[offset].Term, nil
  }

func (l *RaftLog) Append(entries []Entry) {
  	l.mu.Lock()
  	defer l.mu.Unlock()
  	for _, e := range entries {
      		offset := int(e.Index-l.snapIndex) - 1
      		switch {
            		case offset < len(l.entries):
            			l.entries = append(l.entries[:offset], e)
            		case offset == len(l.entries):
            			l.entries = append(l.entries, e)
            		}
      	}
  }

func (l *RaftLog) Entries(lo, hi LogIndex) ([]Entry, error) {
  	l.mu.RLock()
  	defer l.mu.RUnlock()
  	if lo <= l.snapIndex {
      		return nil, ErrCompacted
      	}
  	offsetLo := int(lo-l.snapIndex) - 1
  	offsetHi := int(hi-l.snapIndex) - 1
  	if offsetLo < 0 || offsetHi > len(l.entries) {
      		return nil, errors.New("index out of range")
      	}
  	result := make([]Entry, offsetHi-offsetLo)
  	copy(result, l.entries[offsetLo:offsetHi])
  	return result, nil
  }

func (l *RaftLog) CompactTo(snapIndex LogIndex, snapTerm Term) {
  	l.mu.Lock()
  	defer l.mu.Unlock()
  	if snapIndex <= l.snapIndex {
      		return
      	}
  	offset := int(snapIndex-l.snapIndex) - 1
  	if offset < len(l.entries) {
      		l.entries = l.entries[offset+1:]
      	} else {
      		l.entries = nil
      	}
  	l.snapIndex = snapIndex
  	l.snapTerm = snapTerm
  }
