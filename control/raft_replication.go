package main

import (
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// replicateLogEntry sends a single log entry to all peers and waits for majority replication.
func (n *RaftNode) replicateLogEntry(entry LogEntry) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		log.Printf("[%s] replicateLogEntry called but not leader\n", n.ID)
		return false
	}
	term := n.persistent.CurrentTerm
	n.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 1 // leader itself counts
	totalPeers := len(n.Peers)
	majority := (totalPeers+1)/2 + 1

	for _, peerAddr := range n.Peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			client, err := rpc.Dial("tcp", peer)
			if err != nil {
				return
			}
			defer client.Close()

			args := AppendEntriesArgs{
				Term:     term,
				LeaderID: n.ID,
				Entries:  []LogEntry{entry},
			}
			var reply AppendEntriesReply

			// set timeout context manually
			done := make(chan error, 1)
			go func() { done <- client.Call("RaftRPC.AppendEntries", args, &reply) }()
			select {
			case err = <-done:
			case <-time.After(1 * time.Second):
				err = rpc.ErrShutdown
			}
			if err != nil {
				return
			}

			if reply.Success {
				mu.Lock()
				successCount++
				mu.Unlock()
				log.Printf("[%s] replication to %s succeeded (count=%d/%d required=%d)",
					n.ID, peer, successCount, totalPeers+1, majority)
			}
		}(peerAddr)
	}

	wg.Wait()

	if successCount >= majority {
		log.Printf("[%s] committed up to index %d", n.ID, entry.Index)
		return true
	}
	log.Printf("[%s] replication failed for index %d (%d/%d)", n.ID, entry.Index, successCount, totalPeers+1)
	return false
}

// replicateLogEntries broadcasts the given entries to all followers.
// Returns true if the entry(ies) reach majority and leader advances commitIndex.
func (n *RaftNode) replicateLogEntries(entries []LogEntry) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	term := n.persistent.CurrentTerm
	leaderID := n.ID
	n.mu.Unlock()

	N := 1 + len(n.Peers)
	required := N/2 + 1

	var successCount int32 = 1 // leader already has the entry
	prevIndex := 0
	if len(entries) > 0 {
		prevIndex = entries[0].Index - 1
	}

	for _, peer := range n.Peers {
		go func(peerAddr string) {
			client, err := rpc.Dial("tcp", peerAddr)
			if err != nil {
				return
			}
			defer client.Close()

			prevTerm := 0
			if prevIndex > 0 {
				n.mu.Lock()
				for i := len(n.persistent.Log) - 1; i >= 0; i-- {
					if n.persistent.Log[i].Index == prevIndex {
						prevTerm = n.persistent.Log[i].Term
						break
					}
				}
				n.mu.Unlock()
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     leaderID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries,
				LeaderCommit: n.volatile.commitIndex,
			}
			var reply AppendEntriesReply
			callErr := client.Call("RaftRPC.AppendEntries", args, &reply)
			if callErr != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.persistent.CurrentTerm {
				n.persistent.CurrentTerm = reply.Term
				n.role = Follower
				return
			}

			if reply.Success {
				atomic.AddInt32(&successCount, 1)
				n.matchIndex[peerAddr] = reply.MatchIndex
				n.nextIndex[peerAddr] = reply.MatchIndex + 1
			} else {
				if n.nextIndex[peerAddr] > 1 {
					n.nextIndex[peerAddr]--
				}
			}
		}(peer)
	}

	// wait for majority (max 500ms)
	waited := 0
	for waited < 500 {
		if int(atomic.LoadInt32(&successCount)) >= required {
			n.mu.Lock()
			last := n.persistent.Log[len(n.persistent.Log)-1].Index
			if last > n.volatile.commitIndex {
				n.volatile.commitIndex = last
				// immediately apply entries on leader
				n.applyEntries()
			}
			n.mu.Unlock()
			return true
		}
		time.Sleep(25 * time.Millisecond)
		waited += 25
	}
	return false
}

// applyEntries moves entries from lastApplied+1 .. commitIndex to applyCh (state machine)
func (n *RaftNode) applyEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.volatile.lastApplied < n.volatile.commitIndex {
		n.volatile.lastApplied++
		idx := n.volatile.lastApplied

		var entry LogEntry
		found := false
		for _, e := range n.persistent.Log {
			if e.Index == idx {
				entry = e
				found = true
				break
			}
		}
		if !found {
			log.Printf("[%s] applyEntries: entry index %d not found in log (commitIndex=%d)\n", n.ID, idx, n.volatile.commitIndex)
			continue
		}

		cmd := entry.Command

		// ---- Worker registration (applied on ALL nodes) ----
		if strings.HasPrefix(cmd, "register|") {
			parts := strings.SplitN(cmd, "|", 4)
			if len(parts) != 4 {
				log.Printf("[%s] malformed register command: %s\n", n.ID, cmd)
			} else {
				workerID := parts[1]
				host := parts[2]
				port, err := strconv.Atoi(parts[3])
				if err != nil {
					log.Printf("[%s] invalid register port: %s\n", n.ID, parts[3])
				} else {
					n.Workers[workerID] = WorkerInfo{ID: workerID, Host: host, Port: port}
					n.WorkerLastSeen[workerID] = time.Now()
					log.Printf("[%s] Applied register: %s -> %s:%d\n", n.ID, workerID, host, port)
				}
			}
		}

		// ---- Deploy command (only leader executes) ----
		if strings.HasPrefix(cmd, "deploy|") && n.role == Leader {
			parts := strings.SplitN(cmd, "|", 6)
			if len(parts) != 6 {
				log.Printf("[%s] malformed deploy command: %s\n", n.ID, cmd)
			} else {
				user := parts[1]
				site := parts[2]
				fileURL := parts[3]
				workerID := parts[4]
				portStr := parts[5]

				go func(user, site, fileURL, workerID, portStr string) {
					n.mu.Lock()
					worker, ok := n.Workers[workerID]
					n.mu.Unlock()

					if !ok {
						log.Printf("[%s] deploy: worker %s not found\n", n.ID, workerID)
						return
					}

					addr := fmt.Sprintf("%s:%d", worker.Host, worker.Port+1000)
					client, err := rpc.Dial("tcp", addr)
					if err != nil {
						log.Printf("[%s] deploy: cannot dial worker %s at %s: %v\n", n.ID, workerID, addr, err)
						return
					}
					defer client.Close()

					args := AssignDeploymentArgs{User: user, Site: site, FileURL: fileURL}
					var reply AssignDeploymentReply
					if err := client.Call("WorkerRPC.AssignDeployment", args, &reply); err != nil {
						log.Printf("[%s] deploy RPC error to worker %s: %v\n", n.ID, workerID, err)
						return
					}
					if reply.Success {
						log.Printf("[%s] worker %s deployed %s/%s successfully\n", n.ID, workerID, user, site)
					} else {
						log.Printf("[%s] worker %s deploy failed: %s\n", n.ID, workerID, reply.Message)
					}
				}(user, site, fileURL, workerID, portStr)
			}
		}

		// ---- Send entry to applyCh (non-blocking) ----
		select {
		case n.applyCh <- entry:
		default:
			go func(en LogEntry) { n.applyCh <- en }(entry)
		}
	}
}
