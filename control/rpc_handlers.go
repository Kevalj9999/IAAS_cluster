package main

import (
	"log"
	"time"
)

// ====== AppendEntries (Heartbeat / Replication) ======
func (r *RaftRPC) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.persistent.CurrentTerm
	reply.Success = false
	reply.MatchIndex = 0

	// Step down if leader term higher
	if args.Term > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = args.Term
		n.persistent.VotedFor = ""
		n.role = Follower
		n.leaderID = args.LeaderID
		_ = n.persistState()
		log.Printf("[%s] stepped down, new leader=%s term=%d", n.ID, args.LeaderID, args.Term)
	}

	// Reject older terms
	if args.Term < n.persistent.CurrentTerm {
		return nil
	}

	// Reset election timer
	select {
	case n.heartbeatCh <- true:
	default:
	}
	n.leaderID = args.LeaderID

	// Consistency check
	if args.PrevLogIndex > 0 {
		if len(n.persistent.Log) < args.PrevLogIndex {
			return nil
		}
		if n.persistent.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			n.persistent.Log = n.persistent.Log[:args.PrevLogIndex-1]
			return nil
		}
	}

	// Append new entries
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if len(n.persistent.Log) >= idx {
			if n.persistent.Log[idx-1].Term != entry.Term {
				n.persistent.Log = n.persistent.Log[:idx-1]
				n.persistent.Log = append(n.persistent.Log, entry)
			}
		} else {
			n.persistent.Log = append(n.persistent.Log, entry)
		}
	}

	// Update follower match index
	if len(n.persistent.Log) > 0 {
		reply.MatchIndex = n.persistent.Log[len(n.persistent.Log)-1].Index
	}
	reply.Success = true

	// Commit update
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

	log.Printf("[%s] AppendEntries ok from leader=%s term=%d newIndex=%d commitIndex=%d",
		n.ID, args.LeaderID, args.Term, reply.MatchIndex, n.volatile.commitIndex)
	return nil
}

// ====== SubmitCommand (Client / Internal Command Append) ======
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

	entry := LogEntry{
		Index:   len(n.persistent.Log) + 1,
		Term:    n.persistent.CurrentTerm,
		Command: args.Command,
	}

	n.mu.Lock()
	n.persistent.Log = append(n.persistent.Log, entry)
	n.mu.Unlock()

	go func() {
		ok := n.replicateLogEntry(entry)
		if ok {
			n.mu.Lock()
			n.volatile.commitIndex = entry.Index
			n.mu.Unlock()
			n.applyEntries()
			log.Printf("[%s] committed command: %s", n.ID, args.Command)
		}
	}()

	reply.Success = true
	reply.LeaderID = n.ID
	reply.Message = "accepted"
	return nil
}

// ====== Worker / Node Heartbeat ======
var workerStates = make(map[string]*WorkerState)

func (r *RaftRPC) WorkerHeartbeat(args WorkerHeartbeatArgs, reply *WorkerHeartbeatReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		reply.Success = false
		reply.LeaderID = n.leaderID
		reply.Message = "not leader"
		return nil
	}

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

	if state.SuccessCount >= 2 {
		n.WorkerLastSeen[args.WorkerID] = time.Now()
		if _, ok := n.Workers[args.WorkerID]; ok {
			log.Printf("[%s] heartbeat accepted from %s", n.ID, args.WorkerID)
		}
	}

	reply.Success = true
	reply.LeaderID = n.ID
	reply.Message = "ok"
	return nil
}

// ====== ListWorkers ======
func (r *RaftRPC) ListWorkers(_ struct{}, reply *ListWorkersReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Leader = n.leaderID
	for _, w := range n.Workers {
		reply.Workers = append(reply.Workers, w)
	}
	return nil
}
