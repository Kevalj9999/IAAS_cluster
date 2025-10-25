package main

import (
	"flag"
	"log"
	"math/rand"
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
	node.Start()

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
