// Package metrics exposes Prometheus metrics and OpenTelemetry traces for Raft.
package metrics

import (
  	"github.com/prometheus/client_golang/prometheus"
  	"github.com/prometheus/client_golang/prometheus/promauto"

  	"github.com/pierre21carrion-hash/raft-consensus/internal/raft"
  )

// Metrics holds all Prometheus counters and gauges for a Raft node
type Metrics struct {
  	electionsStarted prometheus.Counter
  	becameLeader     prometheus.Counter
  	commandsProposed prometheus.Counter
  	commitIndex      prometheus.Gauge
  	lastApplied      prometheus.Gauge
  }

// NewMetrics registers and returns a Metrics instance for the given node ID
func NewMetrics(nodeID string) *Metrics {
  	labels := prometheus.Labels{"node": nodeID}
  	return &Metrics{
      		electionsStarted: promauto.NewCounter(prometheus.CounterOpts{
            			Namespace:   "raft",
            			Name:        "elections_started_total",
            			Help:        "Total number of elections started by this node.",
            			ConstLabels: labels,
            		}),
      		becameLeader: promauto.NewCounter(prometheus.CounterOpts{
            			Namespace:   "raft",
            			Name:        "became_leader_total",
            			Help:        "Total number of times this node became leader.",
            			ConstLabels: labels,
            		}),
      		commandsProposed: promauto.NewCounter(prometheus.CounterOpts{
            			Namespace:   "raft",
            			Name:        "commands_proposed_total",
            			Help:        "Total number of commands proposed by this leader.",
            			ConstLabels: labels,
            		}),
      		commitIndex: promauto.NewGauge(prometheus.GaugeOpts{
            			Namespace:   "raft",
            			Name:        "commit_index",
            			Help:        "Current commit index of this node.",
            			ConstLabels: labels,
            		}),
      		lastApplied: promauto.NewGauge(prometheus.GaugeOpts{
            			Namespace:   "raft",
            			Name:        "last_applied",
            			Help:        "Index of the last log entry applied to the state machine.",
            			ConstLabels: labels,
            		}),
      	}
  }

func (m *Metrics) ElectionStarted(term raft.Term) {
  	m.electionsStarted.Inc()
  }

func (m *Metrics) BecameLeader(term raft.Term) {
  	m.becameLeader.Inc()
  }

func (m *Metrics) CommandProposed() {
  	m.commandsProposed.Inc()
  }

func (m *Metrics) CommitIndex(idx raft.LogIndex) {
  	m.commitIndex.Set(float64(idx))
  }

func (m *Metrics) Applied(idx raft.LogIndex) {
  	m.lastApplied.Set(float64(idx))
  }
