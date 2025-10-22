package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// DeploySiteWithURL instructs the cluster leader to replicate and assign deployment.
func (n *RaftNode) DeploySiteWithURL(user, site, fileURL string) error {
	n.mu.Lock()
	if n.role != Leader {
		role := n.role
		n.mu.Unlock()
		return fmt.Errorf("not leader, current role=%s", role)
	}

	// pick an active worker (recent heartbeat)
	now := time.Now()
	activeWorkers := []WorkerInfo{}
	for id, info := range n.Workers {
		last, ok := n.WorkerLastSeen[id]
		if ok && now.Sub(last) <= 10*time.Second {
			activeWorkers = append(activeWorkers, info)
		}
	}

	// fallback: if none are "active", use any known worker
	if len(activeWorkers) == 0 {
		for _, info := range n.Workers {
			activeWorkers = append(activeWorkers, info)
		}
	}

	if len(activeWorkers) == 0 {
		n.mu.Unlock()
		return fmt.Errorf("no workers available for deployment")
	}

	// choose random worker to balance load
	chosen := activeWorkers[rand.Intn(len(activeWorkers))]
	n.mu.Unlock()

	// ✅ use '|' delimiter to avoid URL colon issues
	cmd := fmt.Sprintf("deploy|%s|%s|%s|%s|%d", user, site, fileURL, chosen.ID, chosen.Port)

	args := SubmitCommandArgs{Command: cmd}
	var reply SubmitCommandReply

	// reuse existing command submit logic
	rpcObj := RaftRPC{node: n}
	if err := rpcObj.SubmitCommand(args, &reply); err != nil {
		return fmt.Errorf("SubmitCommand RPC error: %v", err)
	}
	if !reply.Success {
		return fmt.Errorf("SubmitCommand failed: leader=%s msg=%s", reply.LeaderID, reply.Message)
	}

	log.Printf("[%s] Deploy command appended: user=%s site=%s -> worker=%s\n",
		n.ID, user, site, chosen.ID)
	return nil
}
