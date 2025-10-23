package main

import (
	"log"
	"time"
)

// RequestVote remains same as earlier (omitted here for brevity if already present)

func (r *RaftRPC) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.persistent.CurrentTerm
	reply.Success = false
	reply.MatchIndex = 0

	// 1) Step down if term higher
	if args.Term > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = args.Term
		n.persistent.VotedFor = ""
		n.role = Follower
		n.leaderID = args.LeaderID
		_ = n.persistState()
		log.Printf("[%s] stepping down, new leader=%s term=%d\n", n.ID, args.LeaderID, args.Term)
	}

	// 2) Only accept heartbeat from current or higher term leader
	if args.Term < n.persistent.CurrentTerm {
		return nil
	}

	// 3) reset election timer immediately
	n.heartbeatCh <- true
	n.leaderID = args.LeaderID

	// reset election timer
	go func() { n.heartbeatCh <- true }()

	// 3) Consistency check: do we have entry at PrevLogIndex with PrevLogTerm?
	if args.PrevLogIndex > 0 {
		if len(n.persistent.Log) < args.PrevLogIndex {
			// follower missing entries; reject
			return nil
		}
		if n.persistent.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// conflict: delete entry and all after it
			n.persistent.Log = n.persistent.Log[:args.PrevLogIndex-1]
			return nil
		}
	}

	// 4) Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if len(n.persistent.Log) >= idx {
			// overwrite if conflict
			if n.persistent.Log[idx-1].Term != entry.Term {
				n.persistent.Log = n.persistent.Log[:idx-1]
				n.persistent.Log = append(n.persistent.Log, entry)
			}
		} else {
			// append new entry
			n.persistent.Log = append(n.persistent.Log, entry)
		}
	}

	// update MatchIndex
	if len(n.persistent.Log) > 0 {
		reply.MatchIndex = n.persistent.Log[len(n.persistent.Log)-1].Index
	} else {
		reply.MatchIndex = 0
	}
	reply.Success = true

	// 5) Update commitIndex
	last := 0
	if len(n.persistent.Log) > 0 {
		last = n.persistent.Log[len(n.persistent.Log)-1].Index
	}
	if args.LeaderCommit > n.volatile.commitIndex {
		if args.LeaderCommit < last {
			n.volatile.commitIndex = args.LeaderCommit
		} else {
			n.volatile.commitIndex = last
		}
		n.applyEntries()
	}

	log.Printf("[%s] AppendEntries accepted from leader %s term=%d newLastIndex=%d commitIndex=%d\n",
		n.ID, args.LeaderID, args.Term, reply.MatchIndex, n.volatile.commitIndex)

	return nil
}

// SubmitCommand RPC handler: clients call this to submit commands to the cluster.
// If the node is leader, it appends and tries to replicate; otherwise reply with leader address.
type SubmitCommandArgs struct {
	Command string
}

type SubmitCommandReply struct {
	Success  bool
	LeaderID string // id of leader (if known)
	Message  string
}

func (r *RaftRPC) SubmitCommand(args SubmitCommandArgs, reply *SubmitCommandReply) error {
	n := r.node
	n.mu.Lock()
	isLeader := n.role == Leader
	n.mu.Unlock()

	if !isLeader {
		reply.Success = false
		reply.LeaderID = n.leaderID
		reply.Message = "not leader"
		return nil
	}

	// append registration command to log
	entry := LogEntry{
		Index:   len(n.persistent.Log) + 1,
		Term:    n.persistent.CurrentTerm,
		Command: args.Command, // could be "register|workerID|host|port"
	}

	n.mu.Lock()
	n.persistent.Log = append(n.persistent.Log, entry)
	n.mu.Unlock()

	// replicate asynchronously
	go func() {
		ok := n.replicateLogEntry(entry)
		if ok {
			n.mu.Lock()
			n.volatile.commitIndex = entry.Index
			n.mu.Unlock()
			n.applyEntries()
			log.Printf("[%s] committed worker registration: %s", n.ID, args.Command)
		}
	}()

	reply.Success = true
	reply.LeaderID = n.ID
	reply.Message = "log accepted"
	return nil
}

// WorkerHeartbeatArgs / Reply
type WorkerHeartbeatArgs struct {
	WorkerID string
	Host     string
	Port     int
}

type WorkerHeartbeatReply struct {
	Success  bool
	LeaderID string // empty if this node is leader and accepted
	Message  string
}

// ListWorkersReply for client inspection
type ListWorkersReply struct {
	Workers []WorkerInfo
	Leader  string
}

// WorkerHeartbeat RPC: workers call this frequently.
// If this node is leader, it updates WorkerLastSeen (volatile). If not leader, it returns LeaderID empty (or known)
var workerStates = make(map[string]*WorkerState) // key: workerID

func (r *RaftRPC) WorkerHeartbeat(args WorkerHeartbeatArgs, reply *WorkerHeartbeatReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		// Tell worker who the current leader is
		reply.Success = false
		reply.LeaderID = n.leaderID
		reply.Message = "not leader"
		return nil
	}

	// Leader accepts heartbeat
	state, ok := workerStates[args.WorkerID]
	if !ok {
		state = &WorkerState{LeaderID: n.ID, SuccessCount: 0}
		workerStates[args.WorkerID] = state
	}

	if state.LeaderID == n.ID {
		state.SuccessCount++
	} else {
		state.LeaderID = n.ID
		state.SuccessCount = 1
	}

	// Only mark worker as "trusting this leader" after 2 consecutive heartbeats
	if state.SuccessCount >= 2 {
		n.WorkerLastSeen[args.WorkerID] = time.Now()
		if _, ok := n.Workers[args.WorkerID]; ok {
			log.Printf("[%s] heartbeat accepted from worker %s\n", n.ID, args.WorkerID)
		}
	}

	reply.Success = true
	reply.LeaderID = n.ID
	reply.Message = "ok"
	return nil
}

// ListWorkers RPC: returns the persisted workers list (from Raft log). Always served.
func (r *RaftRPC) ListWorkers(_ struct{}, reply *ListWorkersReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()
	res := make([]WorkerInfo, 0, len(n.Workers))
	for _, w := range n.Workers {
		res = append(res, w)
	}
	reply.Workers = res
	reply.Leader = n.leaderID
	return nil
}
