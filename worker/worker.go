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

func dialRPC(addr string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

func callWithTimeout(client *rpc.Client, serviceMethod string, args interface{}, reply interface{}, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- client.Call(serviceMethod, args, reply) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
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

	go startWorkerRPC("./sites", *port)

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

	registerCmd := fmt.Sprintf("register|%s|%s|%d", *id, *host, *port)
	var knownLeaderAddr string
	successCount := 0
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
			err = callWithTimeout(client, "RaftRPC.SubmitCommand", args, &reply, 3*time.Second)
			_ = client.Close()
			if err != nil {
				if debug {
					log.Printf("[worker %s] SubmitCommand to %s failed: %v", *id, peer, err)
				}
				continue
			}

			// Update knownLeaderAddr from host:port if reply message contains it
			if reply.Success && strings.Contains(reply.Message, "log accepted") {
				knownLeaderAddr = peer
				if debug {
					log.Printf("[worker %s] registration accepted via %s", *id, peer)
				}
				registered = true
				break
			} else if !reply.Success && reply.LeaderID != "" {
				knownLeaderAddr = reply.LeaderID
				if debug {
					log.Printf("[worker %s] learned leader %s", *id, knownLeaderAddr)
				}
			}
		}

		if !registered {
			time.Sleep(800 * time.Millisecond)
		}
	}

	time.Sleep(300 * time.Millisecond)
	log.Printf("[worker %s] starting heartbeat loop (knownLeader=%s)", *id, knownLeaderAddr)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		tryPeers := controlPeers
		if knownLeaderAddr != "" {
			tryPeers = append([]string{knownLeaderAddr}, tryPeers...)
		}

		for _, peer := range tryPeers {
			client, err := dialRPC(peer, 2*time.Second)
			if err != nil {
				continue
			}
			args := WorkerHeartbeatArgs{WorkerID: *id, Host: *host, Port: *port}
			var hbReply WorkerHeartbeatReply
			err = callWithTimeout(client, "RaftRPC.WorkerHeartbeat", args, &hbReply, 3*time.Second)
			_ = client.Close()
			if err != nil || !hbReply.Success {
				continue
			}

			if hbReply.LeaderID == knownLeaderAddr {
				successCount++
			} else {
				knownLeaderAddr = hbReply.LeaderID
				successCount = 1
			}

			if successCount >= 2 {
				log.Printf("[worker %s] leader confirmed: %s\n", *id, knownLeaderAddr)
			}
			break
		}
	}
}
