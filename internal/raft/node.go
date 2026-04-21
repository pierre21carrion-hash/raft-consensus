package raft

import (
  	"time"

  	"go.uber.org/zap"
  )

func NewRaftNode(cfg Config, applyCh chan ApplyMsg) (*RaftNode, error) {
  	wal, err := NewWAL(cfg.DataDir)
  	if err != nil {
      		return nil, err
      	}
  	term, votedFor, entries := wal.LoadState()
  	log := NewRaftLog()
  	log.Append(entries)

  	snap, err := NewSnapshotter(cfg.DataDir)
  	if err != nil {
      		return nil, err
      	}

  	metrics := NewMetrics(string(cfg.ID))

  	n := &RaftNode{
      		config:      cfg,
      		currentTerm: term,
      		votedFor:    votedFor,
      		log:         log,
      		state:       Follower,
      		applyCh:     applyCh,
      		stopCh:      make(chan struct{}),
      		electionCh:  make(chan struct{}, 1),
      		heartbeatCh: make(chan struct{}, 1),
      		wal:         wal,
      		snapshotter: snap,
      		metrics:     metrics,
      		lastContact: time.Now(),
      	}
  	n.membership = NewMembershipManager(cfg.ID, cfg.Peers)
  	n.rpcServer = NewRPCServer(n, cfg.ListenAddr)
  	return n, nil
  }

func (n *RaftNode) Start() {
  	go n.runElectionTimer()
  	go n.applyLoop()
  	n.rpcServer.Start()
  	zap.L().Info("raft node started", zap.String("id", string(n.config.ID)))
  }

func (n *RaftNode) Stop() {
  	close(n.stopCh)
  	n.rpcServer.Stop()
  }

func (n *RaftNode) Propose(command []byte) (LogIndex, Term, bool) {
  	n.mu.Lock()
  	defer n.mu.Unlock()
  	if n.state != Leader {
      		return 0, 0, false
      	}
  	idx := n.log.lastIndex() + 1
  	entry := Entry{Index: idx, Term: n.currentTerm, Command: command}
  	n.log.Append([]Entry{entry})
  	n.wal.AppendEntry(entry)
  	n.leaderState.MatchIndex[n.config.ID] = idx
  	n.metrics.CommandProposed()
  	return idx, n.currentTerm, true
  }

func (n *RaftNode) runHeartbeat() {
  	ticker := time.NewTicker(time.Duration(n.config.HeartbeatMs) * time.Millisecond)
  	defer ticker.Stop()
  	for {
      		select {
            		case <-n.stopCh:
            			return
            		case <-ticker.C:
            			n.mu.RLock()
            			if n.state != Leader {
                    				n.mu.RUnlock()
                    				return
                    			}
            			peers := n.membership.Peers()
            			n.mu.RUnlock()
            			for _, peer := range peers {
                    				go n.replicateTo(peer)
                    			}
            		}
      	}
  }

func (n *RaftNode) replicateTo(peer NodeID) {
  	n.mu.Lock()
  	if n.state != Leader {
      		n.mu.Unlock()
      		return
      	}
  	nextIdx := n.leaderState.NextIndex[peer]
  	snapIndex, snapTerm, snapData := n.snapshotter.Latest()
  	if nextIdx <= snapIndex {
      		n.mu.Unlock()
      		n.sendSnapshot(peer, snapIndex, snapTerm, snapData)
      		return
      	}
  	prevLogIndex := nextIdx - 1
  	prevLogTerm, _ := n.log.Term(prevLogIndex)
  	entries, _ := n.log.Entries(nextIdx, n.log.lastIndex()+1)
  	leaderCommit := n.commitIndex
  	term := n.currentTerm
  	n.mu.Unlock()

  	reply := n.rpcServer.AppendEntries(peer, AppendEntriesArgs{
      		Term:         term,
      		LeaderID:     n.config.ID,
      		PrevLogIndex: prevLogIndex,
      		PrevLogTerm:  prevLogTerm,
      		Entries:      entries,
      		LeaderCommit: leaderCommit,
      	})

  	n.mu.Lock()
  	defer n.mu.Unlock()
  	if reply.Term > n.currentTerm {
      		n.stepDown(reply.Term)
      		return
      	}
  	if n.state != Leader || reply.Term != n.currentTerm {
      		return
      	}
  	if reply.Success {
      		if len(entries) > 0 {
            			newMatch := entries[len(entries)-1].Index
            			if newMatch > n.leaderState.MatchIndex[peer] {
                    				n.leaderState.MatchIndex[peer] = newMatch
                    			}
            			n.leaderState.NextIndex[peer] = newMatch + 1
            		}
      		n.maybeAdvanceCommit()
      	} else {
      		if reply.ConflictIndex > 0 {
            			n.leaderState.NextIndex[peer] = reply.ConflictIndex
            		} else if n.leaderState.NextIndex[peer] > 1 {
            			n.leaderState.NextIndex[peer]--
            		}
      	}
  }

func (n *RaftNode) maybeAdvanceCommit() {
  	peers := n.membership.Peers()
  	quorum := (len(peers)+1)/2 + 1
  	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
      		t, err := n.log.Term(idx)
      		if err != nil || t != n.currentTerm {
            			continue
            		}
      		count := 1
      		for _, p := range peers {
            			if n.leaderState.MatchIndex[p] >= idx {
                    				count++
                    			}
            		}
      		if count >= quorum {
            			n.commitIndex = idx
            			n.metrics.CommitIndex(idx)
            			select {
                    			case n.heartbeatCh <- struct{}{}:
                    			default:
                    			}
            			break
            		}
      	}
  }

func (n *RaftNode) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
  	n.mu.Lock()
  	defer n.mu.Unlock()
  	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}
  	if args.Term < n.currentTerm {
      		return reply
      	}
  	if args.Term > n.currentTerm {
      		n.stepDown(args.Term)
      	}
  	n.state = Follower
  	n.leaderID = args.LeaderID
  	n.lastContact = time.Now()
  	select {
      	case n.electionCh <- struct{}{}:
      	default:
      	}
  	if args.PrevLogIndex > 0 {
      		prevTerm, err := n.log.Term(args.PrevLogIndex)
      		if err != nil || prevTerm != args.PrevLogTerm {
            			reply.ConflictIndex = n.log.LastIndex()
            			return reply
            		}
      	}
  	if len(args.Entries) > 0 {
      		n.log.Append(args.Entries)
      		for _, e := range args.Entries {
            			n.wal.AppendEntry(e)
            		}
      	}
  	if args.LeaderCommit > n.commitIndex {
      		if args.LeaderCommit < n.log.LastIndex() {
            			n.commitIndex = args.LeaderCommit
            		} else {
            			n.commitIndex = n.log.LastIndex()
            		}
      		n.metrics.CommitIndex(n.commitIndex)
      	}
  	reply.Success = true
  	reply.Term = n.currentTerm
  	return reply
  }

func (n *RaftNode) applyLoop() {
  	for {
      		select {
            		case <-n.stopCh:
            			return
            		case <-n.heartbeatCh:
            		case <-time.After(10 * time.Millisecond):
            		}
      		n.mu.Lock()
      		for n.lastApplied < n.commitIndex {
            			n.lastApplied++
            			entries, err := n.log.Entries(n.lastApplied, n.lastApplied+1)
            			if err != nil {
                    				break
                    			}
            			n.mu.Unlock()
            			n.applyCh <- ApplyMsg{Index: entries[0].Index, Command: entries[0].Command}
            			n.mu.Lock()
            			n.metrics.Applied(n.lastApplied)
            			if n.lastApplied%LogIndex(n.config.SnapshotThreshold) == 0 {
                    				go n.takeSnapshot(n.lastApplied)
                    			}
            		}
      		n.mu.Unlock()
      	}
  }
