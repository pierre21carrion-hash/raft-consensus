package raft

import (
  	"encoding/json"
  	"os"
  	"path/filepath"
  	"sync"
  	"time"
  )

type SnapshotMeta struct {
  	Index LogIndex `json:"index"`
  	Term  Term     `json:"term"`
  }

type Snapshotter struct {
  	mu    sync.RWMutex
  	dir   string
  	index LogIndex
  	term  Term
  	data  []byte
  }

func NewSnapshotter(dir string) (*Snapshotter, error) {
  	if err := os.MkdirAll(dir, 0755); err != nil {
      		return nil, err
      	}
  	s := &Snapshotter{dir: dir}
  	_ = s.load()
  	return s, nil
  }

func (s *Snapshotter) Latest() (LogIndex, Term, []byte) {
  	s.mu.RLock()
  	defer s.mu.RUnlock()
  	return s.index, s.term, s.data
  }

func (s *Snapshotter) Save(index LogIndex, term Term, data []byte) error {
  	s.mu.Lock()
  	defer s.mu.Unlock()
  	meta := SnapshotMeta{Index: index, Term: term}
  	metaBytes, err := json.Marshal(meta)
  	if err != nil {
      		return err
      	}
  	if err := os.WriteFile(filepath.Join(s.dir, "snapshot.meta"), metaBytes, 0644); err != nil {
      		return err
      	}
  	if err := os.WriteFile(filepath.Join(s.dir, "snapshot.data"), data, 0644); err != nil {
      		return err
      	}
  	s.index = index
  	s.term = term
  	s.data = data
  	return nil
  }

func (s *Snapshotter) load() error {
  	metaBytes, err := os.ReadFile(filepath.Join(s.dir, "snapshot.meta"))
  	if err != nil {
      		return err
      	}
  	var meta SnapshotMeta
  	if err := json.Unmarshal(metaBytes, &meta); err != nil {
      		return err
      	}
  	data, err := os.ReadFile(filepath.Join(s.dir, "snapshot.data"))
  	if err != nil {
      		return err
      	}
  	s.index = meta.Index
  	s.term = meta.Term
  	s.data = data
  	return nil
  }

func (n *RaftNode) takeSnapshot(upTo LogIndex) {
  	snapData := []byte("state-machine-snapshot")
  	n.mu.Lock()
  	term, _ := n.log.Term(upTo)
  	n.mu.Unlock()
  	if err := n.snapshotter.Save(upTo, term, snapData); err != nil {
      		return
      	}
  	n.mu.Lock()
  	n.log.CompactTo(upTo, term)
  	n.mu.Unlock()
  }

func (n *RaftNode) sendSnapshot(peer NodeID, idx LogIndex, term Term, data []byte) {
  	n.mu.RLock()
  	currentTerm := n.currentTerm
  	n.mu.RUnlock()
  	reply := n.rpcServer.InstallSnapshot(peer, InstallSnapshotArgs{
      		Term:              currentTerm,
      		LeaderID:          n.config.ID,
      		LastIncludedIndex: idx,
      		LastIncludedTerm:  term,
      		Data:              data,
      	})
  	n.mu.Lock()
  	defer n.mu.Unlock()
  	if reply.Term > n.currentTerm {
      		n.stepDown(reply.Term)
      	}
  }

func (n *RaftNode) HandleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
  	n.mu.Lock()
  	defer n.mu.Unlock()
  	reply := InstallSnapshotReply{Term: n.currentTerm}
  	if args.Term < n.currentTerm {
      		return reply
      	}
  	if args.Term > n.currentTerm {
      		n.stepDown(args.Term)
      	}
  	n.lastContact = time.Now()
  	_ = n.snapshotter.Save(args.LastIncludedIndex, args.LastIncludedTerm, args.Data)
  	n.log.CompactTo(args.LastIncludedIndex, args.LastIncludedTerm)
  	if args.LastIncludedIndex > n.commitIndex {
      		n.commitIndex = args.LastIncludedIndex
      	}
  	n.applyCh <- ApplyMsg{
      		IsSnap: true,
      		Snap:   args.Data,
      		Index:  args.LastIncludedIndex,
      	}
  	return reply
  }
