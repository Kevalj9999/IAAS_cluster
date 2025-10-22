package main

import (
	"log"
	"math/rand"
	"net/rpc"
	"sync/atomic"
	"time"
)

// ============ RPC Service Definition ============

type RaftRPC struct {
	node *RaftNode
}

// ----------- (3b) RequestVote with persistence -----------
func (r *RaftRPC) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	n := r.node
	n.mu.Lock()
	defer n.mu.Unlock()

	reply.Term = n.persistent.CurrentTerm
	reply.VoteGranted = false

	if args.Term < n.persistent.CurrentTerm {
		return nil
	}

	// if candidate term higher, update term and persist
	if args.Term > n.persistent.CurrentTerm {
		n.persistent.CurrentTerm = args.Term
		n.persistent.VotedFor = ""
		n.role = Follower
		if err := n.persistState(); err != nil {
			log.Printf("[%s] persist after term update error: %v\n", n.ID, err)
		}
	}

	if n.persistent.VotedFor == "" || n.persistent.VotedFor == args.CandidateID {
		n.persistent.VotedFor = args.CandidateID
		reply.VoteGranted = true

		// ✅ (3b) persist after granting vote
		if err := n.persistState(); err != nil {
			log.Printf("[%s] persist after vote error: %v\n", n.ID, err)
		}

		go func() { n.grantVoteCh <- true }()
	}

	reply.Term = n.persistent.CurrentTerm
	return nil
}

func NewRaftNode(id string, port int, peers []string) *RaftNode {
	n := &RaftNode{
		ID:             id,
		Port:           port,
		Peers:          peers,
		role:           Follower,
		persistent:     PersistentState{CurrentTerm: 0, VotedFor: "", Log: []LogEntry{}},
		grantVoteCh:    make(chan bool, 1),
		heartbeatCh:    make(chan bool, 1),
		leaderChangeCh: make(chan RaftRole, 1),
		stopCh:         make(chan struct{}),
		applyCh:        make(chan LogEntry, 100),
		StateFile:      stateFilename(id),

		// ✅ initialize maps
		Workers:        make(map[string]WorkerInfo),
		WorkerLastSeen: make(map[string]time.Time),
		matchIndex:     make(map[string]int),
		nextIndex:      make(map[string]int),
	}
	// load previous state
	if err := n.loadState(); err != nil {
		log.Printf("[%s] loadState error: %v\n", id, err)
	} else {
		log.Printf("[%s] loaded state: term=%d votedFor=%s logLen=%d\n",
			id, n.persistent.CurrentTerm, n.persistent.VotedFor, len(n.persistent.Log))
	}
	return n
}

func (n *RaftNode) Start() {
	go n.startRPCServer()
	go n.electionLoop()
	go n.roleLoop()
}

func (n *RaftNode) Stop() { close(n.stopCh) }

// =============================================================
// Election Logic (3a persistence included)
// =============================================================

func (n *RaftNode) electionLoop() {
	resetTimer := func() {
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		const (
			MinElectionTimeout = 5 * time.Second
			MaxElectionTimeout = 10 * time.Second
		)

		timeout := MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))

		n.electionTimer = time.NewTimer(timeout)
	}
	resetTimer()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.heartbeatCh:
			if n.role != Leader {
				resetTimer()
			}
		case <-n.grantVoteCh:
			resetTimer()
		case <-n.electionTimer.C:
			go n.startElection()
			resetTimer()
		}
	}
}

func (n *RaftNode) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.persistent.CurrentTerm++
	n.persistent.VotedFor = n.ID
	term := n.persistent.CurrentTerm

	// ✅ (3a) persist new term + vote
	if err := n.persistState(); err != nil {
		log.Printf("[%s] persist after startElection error: %v\n", n.ID, err)
	}
	n.mu.Unlock()

	log.Printf("[%s] Starting election for term %d\n", n.ID, term)

	votes := int32(1)
	for _, peer := range n.Peers {
		go func(peerAddr string) {
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				log.Printf("[%s] Failed to connect to peer %s: %v\n", n.ID, peerAddr, err)
				return
			}
			defer client.Close()

			args := RequestVoteArgs{Term: term, CandidateID: n.ID}
			var reply RequestVoteReply
			if err := client.Call("RaftRPC.RequestVote", args, &reply); err != nil {
				return
			}

			n.mu.Lock()
			if reply.Term > n.persistent.CurrentTerm {
				n.persistent.CurrentTerm = reply.Term
				n.role = Follower
				if err := n.persistState(); err != nil {
					log.Printf("[%s] persist after higher-term error: %v\n", n.ID, err)
				}
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				atomic.AddInt32(&votes, 1)
			}
		}(peer)
	}

	time.Sleep(350 * time.Millisecond)

	nTotal := 1 + len(n.Peers)
	if int(atomic.LoadInt32(&votes))*2 > nTotal {
		n.mu.Lock()
		n.role = Leader
		n.leaderID = n.ID
		n.nextIndex = make(map[string]int)
		n.matchIndex = make(map[string]int)
		lastIndex := 0
		if len(n.persistent.Log) > 0 {
			lastIndex = n.persistent.Log[len(n.persistent.Log)-1].Index
		}
		for _, peer := range n.Peers {
			n.nextIndex[peer] = lastIndex + 1
			n.matchIndex[peer] = 0
		}
		n.mu.Unlock()

		log.Printf("[%s] Became leader for term %d (votes=%d/%d)\n",
			n.ID, term, votes, nTotal)
		n.leaderChangeCh <- Leader
	} else {
		n.mu.Lock()
		n.role = Follower
		n.mu.Unlock()
	}
}

func (n *RaftNode) roleLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case role := <-n.leaderChangeCh:
			if role == Leader {
				go n.leaderHeartbeater()
			}
		}
	}
}

func (n *RaftNode) leaderHeartbeater() {
	log.Printf("[%s] Starting heartbeats\n", n.ID)
	ticker := time.NewTicker(300 * time.Millisecond)

	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				log.Printf("[%s] stopping heartbeats (role=%s)\n", n.ID, n.role)
				return
			}
			term := n.persistent.CurrentTerm
			n.mu.Unlock()

			for _, peer := range n.Peers {
				go func(peerAddr string) {
					client, err := rpc.Dial("tcp", peerAddr)
					if err != nil {
						return
					}
					defer client.Close()

					args := AppendEntriesArgs{Term: term, LeaderID: n.ID}
					var reply AppendEntriesReply
					_ = client.Call("RaftRPC.AppendEntries", args, &reply)
					if reply.Term > term {
						n.mu.Lock()
						n.persistent.CurrentTerm = reply.Term
						n.role = Follower
						if err := n.persistState(); err != nil {
							log.Printf("[%s] persist after heartbeat term update: %v\n", n.ID, err)
						}
						n.mu.Unlock()
					}
				}(peer)
			}
		}
	}
}
