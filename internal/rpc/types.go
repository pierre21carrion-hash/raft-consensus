package rpc

import "github.com/pierre21carrion-hash/raft-consensus/internal/raft"

// RequestVoteArgs is sent by candidates during an election
type RequestVoteArgs = raft.RequestVoteArgs
type RequestVoteReply = raft.RequestVoteReply

// AppendEntriesArgs is used for log replication and heartbeats
type AppendEntriesArgs = raft.AppendEntriesArgs
type AppendEntriesReply = raft.AppendEntriesReply

// InstallSnapshotArgs transfers a snapshot to a lagging follower
type InstallSnapshotArgs = raft.InstallSnapshotArgs
type InstallSnapshotReply = raft.InstallSnapshotReply
