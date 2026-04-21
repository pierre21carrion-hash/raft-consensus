package raft

import (
  	"sync"
  	"time"
  )

type NodeState int

const (
  	Follower NodeState = iota
  	Candidate
  	Leader
  )

func (s NodeState) String() string {
  	switch s {
      	case Follower:
      		return "follower"
      	case Candidate:
      		return "candidate"
      	case Leader:
      		return "leader"
      	default:
      		return "unknown"
      	}
  }

type Term uint64
type NodeID string
type LogIndex uint64

type Entry struct {
  	Index   LogIndex
  	Term    Term
  	Command []byte
  }

type PersistentState struct {
  	CurrentTerm Term
  	VotedFor    NodeID
  	Log         []Entry
  }

type VolatileState struct {
  	CommitIndex LogIndex
  	LastApplied LogIndex
  }

type LeaderState struct {
  	NextIndex  map[NodeID]LogIndex
  	MatchIndex map[NodeID]LogIndex
  }

type Config struct {
  	ID                NodeID
  	Peers             []NodeID
  	ElectionTimeoutMs int
  	HeartbeatMs       int
  	SnapshotThreshold int
  	DataDir           string
  	ListenAddr        string
  }

type ApplyMsg struct {
  	Index   LogIndex
  	Command []byte
  	IsSnap  bool
  	Snap    []byte
  }

type RaftNode struct {
  	mu     sync.RWMutex
  	config Config

  	currentTerm Term
  	votedFor    NodeID
  	log         *RaftLog

  	commitIndex LogIndex
  	lastApplied LogIndex

  	leaderState *LeaderState
  	state       NodeState
  	leaderID    NodeID

  	applyCh     chan ApplyMsg
  	stopCh      chan struct{}
  	electionCh  chan struct{}
  	heartbeatCh chan struct{}

  	rpcServer   RPCServer
  	wal         *WAL
  	snapshotter *Snapshotter
  	membership  *MembershipManager
  	metrics     *Metrics

  	lastContact time.Time
  }
