package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"strings"
	"time"
)

type SubmitCommandArgs struct{ Command string }
type SubmitCommandReply struct {
	Success  bool
	LeaderID string
	Message  string
}

type WorkerHeartbeatArgs struct {
	WorkerID string
	Host     string
	Port     int
}
type WorkerHeartbeatReply struct {
	Success  bool
	LeaderID string
	Message  string
}

var debug bool

// dialRPC returns an *rpc.Client with dial timeout
func dialRPC(addr string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// callWithTimeout performs client.Call but returns error if timeout exceeded.
func callWithTimeout(client *rpc.Client, serviceMethod string, args interface{}, reply interface{}, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		err := client.Call(serviceMethod, args, reply)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		// best effort close
		_ = client.Close()
		return fmt.Errorf("rpc call timeout after %s", timeout)
	}
}

func main() {
	id := flag.String("id", "worker1", "worker id")
	host := flag.String("host", "localhost", "worker host")
	port := flag.Int("port", 9001, "worker HTTP port")
	control := flag.String("control", "localhost:8101,localhost:8102,localhost:8103", "comma-separated control node RPC addresses")
	flag.BoolVar(&debug, "debug", true, "enable debug logs")
	flag.Parse()

	// start background servers
	go startWorkerRPC("./sites", *port)

	// normalize peers
	controlPeers := []string{}
	for _, p := range strings.Split(*control, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			controlPeers = append(controlPeers, p)
		}
	}
	if len(controlPeers) == 0 {
		log.Fatalf("[worker %s] no control peers provided", *id)
	}
	log.Printf("[worker %s] control peers: %v", *id, controlPeers)

	// registration command (use '|' delimiter)
	registerCmd := fmt.Sprintf("register|%s|%s|%d", *id, *host, *port)

	var knownLeaderAddr string // only keep if it's host:port

	registered := false
	for !registered {
		tryPeers := controlPeers
		if knownLeaderAddr != "" {
			tryPeers = append([]string{knownLeaderAddr}, tryPeers...)
		}
		for _, peer := range tryPeers {
			if debug {
				log.Printf("[worker %s] dialing %s for registration", *id, peer)
			}
			client, err := dialRPC(peer, 2*time.Second)
			if err != nil {
				if debug {
					log.Printf("[worker %s] dial %s failed: %v", *id, peer, err)
				}
				continue
			}

			args := SubmitCommandArgs{Command: registerCmd}
			var reply SubmitCommandReply
			// Make call with a timeout so worker doesn't block forever
			err = callWithTimeout(client, "RaftRPC.SubmitCommand", args, &reply, 1200*time.Millisecond)
			_ = client.Close()
			if err != nil {
				if debug {
					log.Printf("[worker %s] SubmitCommand to %s failed: %v", *id, peer, err)
				}
				continue
			}

			// If reply includes a hint that looks like an address (host:port), use it
			if reply.LeaderID != "" && strings.Contains(reply.LeaderID, ":") {
				knownLeaderAddr = reply.LeaderID
				if debug {
					log.Printf("[worker %s] learned leader address %s", *id, knownLeaderAddr)
				}
			}

			if reply.Success {
				log.Printf("[worker %s] registration accepted (via %s): %s", *id, peer, reply.Message)
				registered = true
				break
			} else {
				log.Printf("[worker %s] submit to %s not leader (leader=%s) msg=%s", *id, peer, reply.LeaderID, reply.Message)
			}
		}
		if !registered {
			time.Sleep(800 * time.Millisecond)
		}
	}

	// give leader a moment to apply registration
	time.Sleep(300 * time.Millisecond)
	log.Printf("[worker %s] starting heartbeat loop (knownLeader=%s)", *id, knownLeaderAddr)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		tryPeers := controlPeers
		if knownLeaderAddr != "" {
			tryPeers = append([]string{knownLeaderAddr}, tryPeers...)
		}
		success := false
		for _, peer := range tryPeers {
			if debug {
				log.Printf("[worker %s] dialing %s for heartbeat", *id, peer)
			}
			client, err := dialRPC(peer, 2*time.Second)
			if err != nil {
				if debug {
					log.Printf("[worker %s] heartbeat dial %s failed: %v", *id, peer, err)
				}
				continue
			}
			args := WorkerHeartbeatArgs{WorkerID: *id, Host: *host, Port: *port}
			var hbReply WorkerHeartbeatReply
			err = callWithTimeout(client, "RaftRPC.WorkerHeartbeat", args, &hbReply, 3*time.Second)
			_ = client.Close()
			if err != nil {
				if debug {
					log.Printf("[worker %s] heartbeat call to %s failed: %v", *id, peer, err)
				}
				continue
			}
			// update known leader address if reply contains host:port
			if hbReply.LeaderID != "" && strings.Contains(hbReply.LeaderID, ":") {
				knownLeaderAddr = hbReply.LeaderID
			}
			if hbReply.Success {
				log.Printf("[worker %s] heartbeat accepted by leader %s (via %s)", *id, hbReply.LeaderID, peer)
				success = true
				break
			} else {
				log.Printf("[worker %s] heartbeat refused by %s (leader=%s) msg=%s", *id, peer, hbReply.LeaderID, hbReply.Message)
			}
		}
		if !success {
			log.Printf("[worker %s] heartbeat: no leader accepted this tick (knownLeader=%s)", *id, knownLeaderAddr)
		}
	}
}
