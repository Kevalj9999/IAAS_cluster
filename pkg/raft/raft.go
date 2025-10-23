package raft

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// RaftNode wraps a Raft instance and helper methods.
type RaftNode struct {
	node   *raft.Raft
	addr   string
	nodeID string
}

// NewRaftNode initializes a new Raft node.
// dataDir - where raft stores data
// raftBind - address for raft (e.g. ":12000")
// nodeID - unique node name
// peers - other node raft addresses
func NewRaftNode(dataDir, raftBind, nodeID string, peers []string) (*RaftNode, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	// Make Raft data directory
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}

	// TCP transport
	addr, err := net.ResolveTCPAddr("tcp", raftBind)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// Snapshot store
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// Log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("stable store: %w", err)
	}

	// FSM (finite state machine)
	fsm := NewFSM()

	// Create Raft instance
	node, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	// Bootstrap cluster
	if len(peers) == 0 {
		// Single-node cluster
		cfg := raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr()},
			},
		}
		if err := node.BootstrapCluster(cfg).Error(); err != nil {
			return nil, fmt.Errorf("bootstrap single-node: %w", err)
		}
		log.Printf("[%s] bootstrapped as single-node cluster", nodeID)
	} else {
		// Multi-node cluster
		var servers []raft.Server
		for _, peer := range peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),      // use peer address as ID
				Address: raft.ServerAddress(peer), // Raft bind address
			})
		}
		// Include self in cluster
		servers = append(servers, raft.Server{
			ID:      config.LocalID,
			Address: transport.LocalAddr(),
		})

		// Bootstrap cluster if fresh
		cfg := raft.Configuration{Servers: servers}
		if err := node.BootstrapCluster(cfg).Error(); err != nil {
			// It's okay if cluster was already bootstrapped
			if !strings.Contains(err.Error(), "cluster has already been bootstrapped") {
				return nil, fmt.Errorf("bootstrap multi-node: %w", err)
			}
		}
		log.Printf("[%s] bootstrapped/joined cluster with peers: %v", nodeID, peers)
	}

	return &RaftNode{
		node:   node,
		addr:   raftBind,
		nodeID: nodeID,
	}, nil
}

// IsLeader returns true if this node is currently the leader.
func (r *RaftNode) IsLeader() bool {
	return r.node.State() == raft.Leader
}

// Leader returns the current leader address.
func (r *RaftNode) Leader() string {
	addr, _ := r.node.LeaderWithID()
	return string(addr)
}

// Propose applies a command to the Raft log.
func (r *RaftNode) Propose(cmd []byte, timeout time.Duration) error {
	f := r.node.Apply(cmd, timeout)
	return f.Error()
}

// Shutdown gracefully shuts down the raft node.
func (r *RaftNode) Shutdown() error {
	f := r.node.Shutdown()
	return f.Error()
}
