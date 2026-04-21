package rpc

import (
  	"context"
  	"net"
  	"time"

  	"go.uber.org/zap"
  	"google.golang.org/grpc"
  	"google.golang.org/grpc/credentials/insecure"

  	"github.com/pierre21carrion-hash/raft-consensus/internal/raft"
  )

// RPCServer is the interface that wraps the Raft RPC layer
type RPCServer interface {
  	Start()
  	Stop()
  	RequestVote(peer raft.NodeID, args raft.RequestVoteArgs) raft.RequestVoteReply
  	AppendEntries(peer raft.NodeID, args raft.AppendEntriesArgs) raft.AppendEntriesReply
  	InstallSnapshot(peer raft.NodeID, args raft.InstallSnapshotArgs) raft.InstallSnapshotReply
  }

// RPCHandler is implemented by the RaftNode
type RPCHandler interface {
  	HandleRequestVote(args raft.RequestVoteArgs) raft.RequestVoteReply
  	HandleAppendEntries(args raft.AppendEntriesArgs) raft.AppendEntriesReply
  	HandleInstallSnapshot(args raft.InstallSnapshotArgs) raft.InstallSnapshotReply
  }

// GRPCRaftServer implements the gRPC server for Raft
type GRPCRaftServer struct {
  	handler    RPCHandler
  	grpcServer *grpc.Server
  	addr       string
  	clients    map[raft.NodeID]*grpc.ClientConn
  }

// NewRPCServer creates a new gRPC-based RPC server
func NewRPCServer(handler RPCHandler, addr string) RPCServer {
  	return &GRPCRaftServer{
      		handler: handler,
      		addr:    addr,
      		clients: make(map[raft.NodeID]*grpc.ClientConn),
      	}
  }

func (s *GRPCRaftServer) Start() {
  	lis, err := net.Listen("tcp", s.addr)
  	if err != nil {
      		zap.L().Fatal("rpc: failed to listen", zap.String("addr", s.addr), zap.Error(err))
      	}
  	s.grpcServer = grpc.NewServer()
  	go func() {
      		if err := s.grpcServer.Serve(lis); err != nil {
            			zap.L().Error("rpc: server error", zap.Error(err))
            		}
      	}()
  	zap.L().Info("rpc: server started", zap.String("addr", s.addr))
  }

func (s *GRPCRaftServer) Stop() {
  	if s.grpcServer != nil {
      		s.grpcServer.GracefulStop()
      	}
  	for _, conn := range s.clients {
      		conn.Close()
      	}
  }

func (s *GRPCRaftServer) getConn(peer raft.NodeID) *grpc.ClientConn {
  	if conn, ok := s.clients[peer]; ok {
      		return conn
      	}
  	conn, err := grpc.NewClient(string(peer),
                                		grpc.WithTransportCredentials(insecure.NewCredentials()),
                                	)
  	if err != nil {
      		zap.L().Error("rpc: dial failed", zap.String("peer", string(peer)), zap.Error(err))
      		return nil
      	}
  	s.clients[peer] = conn
  	return conn
  }

func (s *GRPCRaftServer) RequestVote(peer raft.NodeID, args raft.RequestVoteArgs) raft.RequestVoteReply {
  	// In a full implementation, serialize args and call the remote peer.
  	// Here we return a zero reply to satisfy the interface contract.
  	_ = s.getConn(peer)
  	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
  	defer cancel()
  	_ = ctx
  	return raft.RequestVoteReply{}
  }

func (s *GRPCRaftServer) AppendEntries(peer raft.NodeID, args raft.AppendEntriesArgs) raft.AppendEntriesReply {
  	_ = s.getConn(peer)
  	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
  	defer cancel()
  	_ = ctx
  	return raft.AppendEntriesReply{}
  }

func (s *GRPCRaftServer) InstallSnapshot(peer raft.NodeID, args raft.InstallSnapshotArgs) raft.InstallSnapshotReply {
  	_ = s.getConn(peer)
  	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
  	defer cancel()
  	_ = ctx
  	return raft.InstallSnapshotReply{}
  }
