package raft

import "sync"

type MembershipManager struct {
  	mu       sync.RWMutex
  	selfID   NodeID
  	members  map[NodeID]struct{}
  	jointOld map[NodeID]struct{}
  	jointNew map[NodeID]struct{}
  	inJoint  bool
  }

func NewMembershipManager(self NodeID, peers []NodeID) *MembershipManager {
  	m := &MembershipManager{
      		selfID:  self,
      		members: make(map[NodeID]struct{}),
      	}
  	m.members[self] = struct{}{}
  	for _, p := range peers {
      		m.members[p] = struct{}{}
      	}
  	return m
  }

func (m *MembershipManager) Peers() []NodeID {
  	m.mu.RLock()
  	defer m.mu.RUnlock()
  	peers := make([]NodeID, 0, len(m.members)-1)
  	for id := range m.members {
      		if id != m.selfID {
            			peers = append(peers, id)
            		}
      	}
  	return peers
  }

func (m *MembershipManager) All() []NodeID {
  	m.mu.RLock()
  	defer m.mu.RUnlock()
  	all := make([]NodeID, 0, len(m.members))
  	for id := range m.members {
      		all = append(all, id)
      	}
  	return all
  }

func (m *MembershipManager) BeginJointConsensus(newMembers []NodeID) {
  	m.mu.Lock()
  	defer m.mu.Unlock()
  	old := make(map[NodeID]struct{}, len(m.members))
  	for k, v := range m.members {
      		old[k] = v
      	}
  	nw := make(map[NodeID]struct{}, len(newMembers))
  	for _, id := range newMembers {
      		nw[id] = struct{}{}
      	}
  	m.jointOld = old
  	m.jointNew = nw
  	m.inJoint = true
  }

func (m *MembershipManager) CommitNewConfig() {
  	m.mu.Lock()
  	defer m.mu.Unlock()
  	m.members = m.jointNew
  	m.jointOld = nil
  	m.jointNew = nil
  	m.inJoint = false
  }

func (m *MembershipManager) InJoint() bool {
  	m.mu.RLock()
  	defer m.mu.RUnlock()
  	return m.inJoint
  }

func (m *MembershipManager) QuorumReached(voters map[NodeID]struct{}) bool {
  	m.mu.RLock()
  	defer m.mu.RUnlock()
  	if !m.inJoint {
      		return quorum(m.members, voters)
      	}
  	return quorum(m.jointOld, voters) && quorum(m.jointNew, voters)
  }

func quorum(members, voters map[NodeID]struct{}) bool {
  	needed := len(members)/2 + 1
  	count := 0
  	for id := range voters {
      		if _, ok := members[id]; ok {
            			count++
            		}
      	}
  	return count >= needed
  }
