package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"
)

// startAutoRegistration runs in background to ensure the node is registered to the current leader.
func (n *RaftNode) startAutoRegistration() {
	go func() {
		for {
			select {
			case <-n.stopCh:
				return
			default:
			}

			n.mu.Lock()
			leaderID := n.leaderID
			role := n.role
			port := n.Port
			n.mu.Unlock()

			// if we are leader, ensure self appears in registry
			if role == Leader {
				n.mu.Lock()
				if _, ok := n.Workers[n.ID]; !ok {
					n.Workers[n.ID] = WorkerInfo{ID: n.ID, Host: n.Host, Port: port}
					n.WorkerLastSeen[n.ID] = time.Now()
					log.Printf("[%s] registered self as worker (leader)", n.ID)
				}
				n.mu.Unlock()
				time.Sleep(5 * time.Second)
				continue
			}

			if leaderID == "" {
				time.Sleep(1 * time.Second)
				continue
			}

			leaderAddr := fmt.Sprintf("localhost:%d", 8000+int(leaderID[len(leaderID)-1]-'0'))
			registerCmd := fmt.Sprintf("register|%s|%s|%d", n.ID, n.Host, port)
			args := SubmitCommandArgs{Command: registerCmd}
			var reply SubmitCommandReply

			client, err := rpc.Dial("tcp", leaderAddr)
			if err != nil {
				log.Printf("[%s] cannot reach leader %s for registration: %v", n.ID, leaderAddr, err)
				time.Sleep(2 * time.Second)
				continue
			}

			err = client.Call("RaftRPC.SubmitCommand", args, &reply)
			client.Close()

			if err != nil || !reply.Success {
				log.Printf("[%s] registration to %s failed (%v), retrying...", n.ID, leaderAddr, err)
				time.Sleep(2 * time.Second)
				continue
			}

			log.Printf("[%s] successfully registered to leader %s", n.ID, leaderAddr)
			time.Sleep(8 * time.Second) // re-check every few seconds
		}
	}()
}
