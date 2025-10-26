package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"strings"
	"time"
)

func main() {
	id := flag.String("id", "node1", "node id")
	host := flag.String("host", "localhost", "host/ip to advertise (used in upload URLs)")
	port := flag.Int("port", 8001, "RPC port to listen on")
	peersStr := flag.String("peers", "", "comma separated peer addresses (host:port)")
	sitesDir := flag.String("sites", "./sites", "directory to store hosted sites")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	peerList := []string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			trim := strings.TrimSpace(p)
			if trim != "" {
				peerList = append(peerList, trim)
			}
		}
	}

	// create node with SitesDir
	node := NewRaftNode(*id, *host, *port, peerList, *sitesDir)

	// Add this right below:
	node.PeerIDMap = map[string]string{
		"node1": "localhost:8001",
		"node2": "localhost:8002",
		"node3": "localhost:8003",
	}

	node.Start()
	// ---- Auto self-registration ----
	go func() {
		// initial wait so elections have a chance to finish
		time.Sleep(8 * time.Second)

		registerCmd := fmt.Sprintf("register|%s|%s|%d", node.ID, "localhost", node.Port)
		args := SubmitCommandArgs{Command: registerCmd}
		var reply SubmitCommandReply

		for {
			node.mu.Lock()
			leaderID := node.leaderID
			leaderAddr := ""
			if leaderID != "" {
				if addr, ok := node.PeerIDMap[leaderID]; ok {
					leaderAddr = addr
				}
			}
			node.mu.Unlock()

			if leaderAddr == "" {
				// fall back to self (maybe we're leader)
				leaderAddr = fmt.Sprintf("localhost:%d", node.Port)
			}

			client, err := rpc.Dial("tcp", leaderAddr)
			if err != nil {
				log.Printf("[%s] registration dial failed (%s): %v; retrying...", node.ID, leaderAddr, err)
				time.Sleep(2 * time.Second)
				continue
			}

			// 1) Probe if the target node currently believes it's leader
			var isLead bool
			err = client.Call("RaftRPC.IsLeader", struct{}{}, &isLead)
			if err != nil || !isLead {
				client.Close()
				log.Printf("[%s] target %s not ready as leader (isLeader=%v err=%v), retrying...", node.ID, leaderAddr, isLead, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// 2) Now call SubmitCommand safely
			err = client.Call("RaftRPC.SubmitCommand", args, &reply)
			client.Close()

			if err != nil {
				log.Printf("[%s] registration RPC error to %s: %v; retrying...", node.ID, leaderAddr, err)
				time.Sleep(1 * time.Second)
				continue
			}
			if !reply.Success {
				log.Printf("[%s] self-registration rejected by %s: %s; retrying...", node.ID, leaderAddr, reply.Message)
				time.Sleep(1 * time.Second)
				continue
			}

			log.Printf("[%s] self-registration successful via %s", node.ID, leaderAddr)
			break
		}
	}()

	// start HTTP REST API (serves uploads and deploy endpoint) on port+100
	node.startHTTPServer(*port)

	// print status periodically
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-node.stopCh:
				return
			case <-t.C:
				node.mu.Lock()
				log.Printf("[%s] role=%s term=%d leader=%s workers=%d\n",
					node.ID, node.role, node.persistent.CurrentTerm, node.leaderID, len(node.Workers))
				node.mu.Unlock()
			}
		}
	}()

	// block forever
	select {}
}
