package main

import (
	"sync"
	"time"
)

// RaftRole enumerates node role
type RaftRole string

const (
	Follower  RaftRole = "Follower"
	Candidate RaftRole = "Candidate"
	Leader    RaftRole = "Leader"
)

// LogEntry is a single log entry
type LogEntry struct {
	Index   int
	Term    int
	Command string
}

// PersistentState holds fields that should persist across restarts (simplified)
type PersistentState struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

// VolatileState holds in-memory ephemeral state
type VolatileState struct {
	commitIndex int
	lastApplied int
}

// RaftNode is the primary struct for a node (unified control + worker)
type RaftNode struct {
	mu sync.Mutex

	// identity & network
	ID    string
	Host  string // optional human-readable host
	Port  int    // RPC port (existing code uses this)
	Peers []string

	// where this node serves sites (local filesystem)
	SitesDir string

	// states
	role       RaftRole
	persistent PersistentState
	volatile   VolatileState
	leaderID   string
	StateFile  string

	// leader volatile fields (maintained only on leader)
	nextIndex  map[string]int // next index to send to each peer
	matchIndex map[string]int // highest replicated index on each peer

	// timers & channels
	electionTimer  *time.Timer
	grantVoteCh    chan bool
	heartbeatCh    chan bool
	leaderChangeCh chan RaftRole
	stopCh         chan struct{}

	// apply channel for committed entries -> state machine
	applyCh chan LogEntry

	// Worker registry derived from committed log (persisted entries)
	// In unified nodes, this maps known node IDs to their host/port (optional)
	Workers map[string]WorkerInfo
	// Volatile liveness info maintained by the current leader
	WorkerLastSeen map[string]time.Time
}

// WorkerInfo holds data persisted in the Raft log (ID, host, port)
// In unified nodes this is just peer metadata.
type WorkerInfo struct {
	ID   string
	Host string
	Port int
}

// Volatile worker runtime state (not persisted)
type WorkerState struct {
	LeaderID     string
	SuccessCount int
}
