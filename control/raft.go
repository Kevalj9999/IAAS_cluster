package main

import (
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// ============ RPC Service Definition ============

type RaftRPC struct {
	node *RaftNode
}

// ----------- RequestVote with persistence -----------
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

		// persist after granting vote
		if err := n.persistState(); err != nil {
			log.Printf("[%s] persist after vote error: %v\n", n.ID, err)
		}

		// notify election timer reset
		select {
		case n.grantVoteCh <- true:
		default:
		}
	}

	reply.Term = n.persistent.CurrentTerm
	return nil
}

func NewRaftNode(id, host string, port int, peers []string, sitesDir string) *RaftNode {
	n := &RaftNode{
		ID:             id,
		Host:           host,
		Port:           port,
		Peers:          peers,
		SitesDir:       sitesDir,
		role:           Follower,
		persistent:     PersistentState{CurrentTerm: 0, VotedFor: "", Log: []LogEntry{}},
		grantVoteCh:    make(chan bool, 1),
		heartbeatCh:    make(chan bool, 1),
		leaderChangeCh: make(chan RaftRole, 1),
		stopCh:         make(chan struct{}),
		applyCh:        make(chan LogEntry, 100),
		StateFile:      stateFilename(id),
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
	// start RPC server and election loop
	go n.startRPCServer()
	go n.electionLoop()
	go n.roleLoop()
}

func (n *RaftNode) Stop() { close(n.stopCh) }

// =============================================================
// Election Logic and timers
// =============================================================

func (n *RaftNode) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// Randomized timeout per node - longer timeouts for stability
	const MinElectionTimeout = 2000 * time.Millisecond
	const MaxElectionTimeout = 3500 * time.Millisecond
	timeout := MinElectionTimeout + time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout)))
	n.electionTimer = time.NewTimer(timeout)
}

func (n *RaftNode) electionLoop() {
	n.resetElectionTimer()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.heartbeatCh:
			if n.role != Leader {
				n.resetElectionTimer()
			}
		case <-n.grantVoteCh:
			n.resetElectionTimer()
		case <-n.electionTimer.C:
			go n.startElection()
			n.resetElectionTimer()
		}
	}
}

func (n *RaftNode) startElection() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}
	n.role = Candidate
	n.persistent.CurrentTerm++
	n.persistent.VotedFor = n.ID
	term := n.persistent.CurrentTerm
	// persist changes
	if err := n.persistState(); err != nil {
		log.Printf("[%s] persist error starting election: %v\n", n.ID, err)
	}
	n.mu.Unlock()

	log.Printf("[%s] Starting election for term %d\n", n.ID, term)

	votes := int32(1)
	for _, peer := range n.Peers {
		go func(peerAddr string) {
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
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
				n.persistent.VotedFor = ""
				if err := n.persistState(); err != nil {
					log.Printf("[%s] persist after term update error: %v\n", n.ID, err)
				}
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				atomic.AddInt32(&votes, 1)
			}
		}(peer)
	}

	// wait a short fixed time for votes
	time.Sleep(400 * time.Millisecond)

	nTotal := 1 + len(n.Peers)
	if int(atomic.LoadInt32(&votes))*2 > nTotal {
		n.mu.Lock()
		n.role = Leader
		n.leaderID = n.ID
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
		// notify leader change
		select {
		case n.leaderChangeCh <- Leader:
		default:
		}
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
				// reconstruct worker registry from committed log (no locking of channels here)
				go func() {
					n.mu.Lock()
					defer n.mu.Unlock()
					for idx := 1; idx <= n.volatile.commitIndex; idx++ {
						for _, entry := range n.persistent.Log {
							if entry.Index == idx && strings.HasPrefix(entry.Command, "register|") {
								parts := strings.SplitN(entry.Command, "|", 4)
								if len(parts) == 4 {
									workerID := parts[1]
									host := parts[2]
									port, _ := strconv.Atoi(parts[3])
									n.Workers[workerID] = WorkerInfo{ID: workerID, Host: host, Port: port}
									n.WorkerLastSeen[workerID] = time.Now()
								}
							}
						}
					}
				}()

				go n.leaderHeartbeater()
				go func() {
					time.Sleep(1 * time.Second)
					n.mu.Lock()
					if _, ok := n.Workers[n.ID]; !ok {
						n.Workers[n.ID] = WorkerInfo{ID: n.ID, Host: n.Host, Port: n.Port}
						n.WorkerLastSeen[n.ID] = time.Now()
						log.Printf("[%s] self-registered as leader worker", n.ID)
					}
					n.mu.Unlock()
				}()
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
						n.persistent.VotedFor = ""
						_ = n.persistState()
						n.mu.Unlock()
					}
				}(peer)
			}
		}
	}
}
