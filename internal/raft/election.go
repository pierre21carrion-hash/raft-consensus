package raft

import (
  	"math/rand"
  	"time"

  	"go.uber.org/zap"
  )

func (n *RaftNode) electionTimeout() time.Duration {
  	base := time.Duration(n.config.ElectionTimeoutMs) * time.Millisecond
  	jitter := time.Duration(rand.Int63n(int64(base)))
  	return base + jitter
  }

func (n *RaftNode) runElectionTimer() {
  	for {
      		timeout := n.electionTimeout()
      		select {
            		case <-n.stopCh:
            			return
            		case <-time.After(timeout):
            			n.mu.Lock()
            			state := n.state
            			lastContact := n.lastContact
            			n.mu.Unlock()
            			if state == Leader {
                    				continue
                    			}
            			if time.Since(lastContact) >= timeout {
                    				n.startElection()
                    			}
            		case <-n.electionCh:
            			continue
            		}
      	}
  }

func (n *RaftNode) startElection() {
  	n.mu.Lock()
  	n.state = Candidate
  	n.currentTerm++
  	n.votedFor = n.config.ID
  	term := n.currentTerm
  	lastLogIndex := n.log.LastIndex()
  	lastLogTerm := n.log.LastTerm()
  	peers := n.membership.Peers()
  	n.mu.Unlock()

  	n.metrics.ElectionStarted(term)
  	n.wal.SaveState(term, n.config.ID)

  	zap.L().Info("starting election",
                 		zap.String("node", string(n.config.ID)),
                 		zap.Uint64("term", uint64(term)),
                 	)

  	votes := 1
  	voteCh := make(chan bool, len(peers))

  	for _, peer := range peers {
      		go func(p NodeID) {
            			granted := n.rpcServer.RequestVote(p, RequestVoteArgs{
                    				Term:         term,
                    				CandidateID:  n.config.ID,
                    				LastLogIndex: lastLogIndex,
                    				LastLogTerm:  lastLogTerm,
                    			})
            			voteCh <- granted
            		}(peer)
      	}

  	quorum := (len(peers)+1)/2 + 1

  	for i := 0; i < len(peers); i++ {
      		if <-voteCh {
            			votes++
            		}
      		if votes >= quorum {
            			n.becomeLeader()
            			return
            		}
      	}

  	n.mu.Lock()
  	if n.state == Candidate {
      		n.state = Follower
      	}
  	n.mu.Unlock()
  }

func (n *RaftNode) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
  	n.mu.Lock()
  	defer n.mu.Unlock()

  	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

  	if args.Term < n.currentTerm {
      		return reply
      	}
  	if args.Term > n.currentTerm {
      		n.stepDown(args.Term)
      	}

  	if n.votedFor != "" && n.votedFor != args.CandidateID {
      		return reply
      	}

  	myLastTerm := n.log.LastTerm()
  	myLastIndex := n.log.LastIndex()
  	logOK := args.LastLogTerm > myLastTerm ||
  		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

  	if logOK {
      		n.votedFor = args.CandidateID
      		n.wal.SaveState(n.currentTerm, n.votedFor)
      		reply.VoteGranted = true
      		select {
            		case n.electionCh <- struct{}{}:
            		default:
            		}
      	}
  	return reply
  }

func (n *RaftNode) becomeLeader() {
  	n.mu.Lock()
  	defer n.mu.Unlock()

  	if n.state != Candidate {
      		return
      	}

  	n.state = Leader
  	n.leaderID = n.config.ID
  	peers := n.membership.Peers()

  	nextIdx := n.log.lastIndex() + 1
  	ls := &LeaderState{
      		NextIndex:  make(map[NodeID]LogIndex, len(peers)),
      		MatchIndex: make(map[NodeID]LogIndex, len(peers)),
      	}
  	for _, p := range peers {
      		ls.NextIndex[p] = nextIdx
      		ls.MatchIndex[p] = 0
      	}
  	n.leaderState = ls
  	n.metrics.BecameLeader(n.currentTerm)

  	zap.L().Info("became leader",
                 		zap.String("node", string(n.config.ID)),
                 		zap.Uint64("term", uint64(n.currentTerm)),
                 	)

  	go n.runHeartbeat()
  }

func (n *RaftNode) stepDown(term Term) {
  	n.currentTerm = term
  	n.state = Follower
  	n.votedFor = ""
  	n.leaderState = nil
  	n.wal.SaveState(term, "")
  }
