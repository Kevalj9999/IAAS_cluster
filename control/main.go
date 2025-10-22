package main

import (
	"flag"
	"log"
	"strings"
	"time"
)

func main() {
	id := flag.String("id", "node1", "node id")
	port := flag.Int("port", 8001, "port to listen on")
	peersStr := flag.String("peers", "", "comma separated peer addresses (host:port)")
	flag.Parse()

	peerList := []string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			trim := strings.TrimSpace(p)
			if trim != "" {
				peerList = append(peerList, trim)
			}
		}
	}

	node := NewRaftNode(*id, *port, peerList)
	node.Start()

	// start HTTP REST API
	node.startHTTPServer(*port)

	// print status periodically
	go func() {
		for {
			select {
			case <-node.stopCh:
				return
			case <-time.After(2 * time.Second):
				node.mu.Lock()
				log.Printf("[%s] role=%s term=%d leader=%s\n", node.ID, node.role, node.persistent.CurrentTerm, node.leaderID)
				node.mu.Unlock()
			}
		}
	}()

	// block forever (Ctrl+C to exit)
	select {}
}
