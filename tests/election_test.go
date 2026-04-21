package tests

import (
  	"testing"
  	"time"

  	"github.com/stretchr/testify/assert"
  	"github.com/stretchr/testify/require"

  	"github.com/pierre21carrion-hash/raft-consensus/internal/raft"
  )

// newTestNode creates a standalone RaftNode for unit testing
func newTestNode(t *testing.T, id string) (*raft.RaftNode, chan raft.ApplyMsg) {
  	t.Helper()
  	dir := t.TempDir()
  	applyCh := make(chan raft.ApplyMsg, 128)
  	cfg := raft.Config{
      		ID:                raft.NodeID(id),
      		Peers:             nil,
      		ElectionTimeoutMs: 150,
      		HeartbeatMs:       50,
      		SnapshotThreshold: 100,
      		DataDir:           dir,
      		ListenAddr:        ":0",
      	}
  	node, err := raft.NewRaftNode(cfg, applyCh)
  	require.NoError(t, err)
  	return node, applyCh
  }

// TestSingleNodeElection verifies that a single-node cluster elects itself as leader
func TestSingleNodeElection(t *testing.T) {
  	node, _ := newTestNode(t, "node1")
  	node.Start()
  	defer node.Stop()

  	// A single-node cluster should win an election immediately.
  	time.Sleep(500 * time.Millisecond)

  	_, _, isLeader := node.Propose([]byte("hello"))
  	assert.True(t, isLeader, "single node should become leader")
  }

// TestLeaderPropose verifies that a leader can propose a command
func TestLeaderPropose(t *testing.T) {
  	node, applyCh := newTestNode(t, "leader1")
  	node.Start()
  	defer node.Stop()

  	time.Sleep(500 * time.Millisecond)

  	idx, term, ok := node.Propose([]byte("set x 42"))
  	require.True(t, ok, "propose should succeed on leader")
  	assert.Greater(t, uint64(idx), uint64(0), "index should be positive")
  	assert.Greater(t, uint64(term), uint64(0), "term should be positive")

  	// Wait for commit
  	select {
      	case msg := <-applyCh:
      		assert.Equal(t, []byte("set x 42"), msg.Command)
      	case <-time.After(2 * time.Second):
      		t.Fatal("timed out waiting for applied message")
      	}
  }

// TestNonLeaderRejectsPropose verifies that a follower rejects proposals
func TestNonLeaderRejectsPropose(t *testing.T) {
  	dir := t.TempDir()
  	applyCh := make(chan raft.ApplyMsg, 128)
  	cfg := raft.Config{
      		ID:                "follower",
      		Peers:             []raft.NodeID{"node2", "node3"},
      		ElectionTimeoutMs: 10000, // Very long timeout so it stays follower
      		HeartbeatMs:       50,
      		SnapshotThreshold: 100,
      		DataDir:           dir,
      		ListenAddr:        ":0",
      	}
  	node, err := raft.NewRaftNode(cfg, applyCh)
  	require.NoError(t, err)
  	node.Start()
  	defer node.Stop()

  	_, _, isLeader := node.Propose([]byte("should fail"))
  	assert.False(t, isLeader, "follower should not accept proposals")
  }

// TestElectionTermMonotonicity verifies that term only increases
func TestElectionTermMonotonicity(t *testing.T) {
  	node, _ := newTestNode(t, "mono1")
  	node.Start()
  	defer node.Stop()

  	time.Sleep(500 * time.Millisecond)

  	var lastTerm uint64
  	for i := 0; i < 5; i++ {
      		_, term, _ := node.Propose([]byte("cmd"))
      		assert.GreaterOrEqual(t, uint64(term), lastTerm, "term must be monotonically non-decreasing")
      		lastTerm = uint64(term)
      	}
  }
