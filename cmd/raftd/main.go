// Command raftd starts a Raft consensus node.
// Usage: raftd -id node1 -addr :7001 -peers node2=:7002,node3=:7003 -data /var/raft/node1
package main

import (
  	"flag"
  	"log"
  	"net/http"
  	"os"
  	"os/signal"
  	"strings"
  	"syscall"

  	"github.com/prometheus/client_golang/prometheus/promhttp"
  	"go.uber.org/zap"

  	"github.com/pierre21carrion-hash/raft-consensus/internal/raft"
  )

func main() {
  	id := flag.String("id", "node1", "unique node ID")
  	addr := flag.String("addr", ":7001", "listen address for Raft RPC")
  	peersFlag := flag.String("peers", "", "comma-separated peer addresses, e.g. node2=:7002,node3=:7003")
  	dataDir := flag.String("data", "/tmp/raft", "data directory for WAL and snapshots")
  	metricsAddr := flag.String("metrics", ":9091", "Prometheus metrics listen address")
  	electionMs := flag.Int("election-ms", 300, "election timeout base in milliseconds")
  	heartbeatMs := flag.Int("heartbeat-ms", 100, "heartbeat interval in milliseconds")
  	snapThreshold := flag.Int("snap-threshold", 1000, "snapshot after N applied entries")
  	flag.Parse()

  	logger, _ := zap.NewProduction()
  	zap.ReplaceGlobals(logger)
  	defer logger.Sync()

  	// Parse peers
  	var peers []raft.NodeID
  	if *peersFlag != "" {
      		for _, p := range strings.Split(*peersFlag, ",") {
            			parts := strings.SplitN(p, "=", 2)
            			if len(parts) == 2 {
                    				peers = append(peers, raft.NodeID(parts[0]))
                    			}
            		}
      	}

  	cfg := raft.Config{
      		ID:                raft.NodeID(*id),
      		Peers:             peers,
      		ElectionTimeoutMs: *electionMs,
      		HeartbeatMs:       *heartbeatMs,
      		SnapshotThreshold: *snapThreshold,
      		DataDir:           *dataDir,
      		ListenAddr:        *addr,
      	}

  	applyCh := make(chan raft.ApplyMsg, 256)
  	node, err := raft.NewRaftNode(cfg, applyCh)
  	if err != nil {
      		log.Fatalf("failed to create raft node: %v", err)
      	}
  	node.Start()
  	defer node.Stop()

  	// Apply loop — drives the state machine
  	go func() {
      		for msg := range applyCh {
            			if msg.IsSnap {
                    				zap.L().Info("applied snapshot", zap.Uint64("index", uint64(msg.Index)))
                    			} else {
                    				zap.L().Debug("applied command",
                                          					zap.Uint64("index", uint64(msg.Index)),
                                          					zap.ByteString("cmd", msg.Command),
                                          				)
                    			}
            		}
      	}()

  	// Expose Prometheus metrics
  	http.Handle("/metrics", promhttp.Handler())
  	go func() {
      		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
            			zap.L().Error("metrics server error", zap.Error(err))
            		}
      	}()
  	zap.L().Info("raftd started",
                 		zap.String("id", *id),
                 		zap.String("addr", *addr),
                 		zap.String("metrics", *metricsAddr),
                 	)

  	// Wait for shutdown signal
  	quit := make(chan os.Signal, 1)
  	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
  	<-quit
  	zap.L().Info("shutting down raftd")
  }
