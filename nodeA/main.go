package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// ---------- RPC Service ----------
type NodeService struct {
	Name string
}

type PingRequest struct {
	Sender string
}

type PingResponse struct {
	Message string
}

// RPC Method
func (n *NodeService) Ping(req PingRequest, res *PingResponse) error {
	res.Message = fmt.Sprintf("Hello %s, I’m alive!", req.Sender)
	fmt.Printf("[%s] Received ping from %s at %s\n", n.Name, req.Sender, time.Now().Format("15:04:05"))
	return nil
}

// ---------- Node Info ----------
type NodeInfo struct {
	Name    string
	Address string
	Alive   bool
}

var knownNodes = []NodeInfo{}
var mu sync.Mutex

// ---------- Heartbeat ----------
func heartbeat(selfName string, selfAddr string) {
	for {
		mu.Lock()
		for i, node := range knownNodes {
			if node.Address == selfAddr {
				continue // skip self
			}
			client, err := rpc.Dial("tcp", node.Address)
			if err != nil {
				fmt.Printf("[%s] Node %s unreachable\n", selfName, node.Name)
				knownNodes[i].Alive = false
				continue
			}
			var res PingResponse
			err = client.Call("NodeService.Ping", PingRequest{Sender: selfName}, &res)
			if err != nil {
				fmt.Printf("[%s] Node %s failed ping\n", selfName, node.Name)
				knownNodes[i].Alive = false
				continue
			}
			knownNodes[i].Alive = true
			// fmt.Println(res.Message)
		}
		mu.Unlock()
		time.Sleep(3 * time.Second)
	}
}

// ---------- Main ----------
func main() {
	// Customize this node
	selfName := "NodeA"
	selfAddr := "localhost:1234"

	// Add other nodes here
	knownNodes = []NodeInfo{
		{Name: "NodeA", Address: "localhost:1234", Alive: true},
		{Name: "NodeB", Address: "localhost:1235", Alive: true},
		{Name: "NodeC", Address: "localhost:1236", Alive: true},
	}

	// Start RPC Server
	nodeService := &NodeService{Name: selfName}
	rpc.Register(nodeService)
	listener, err := net.Listen("tcp", selfAddr)
	if err != nil {
		fmt.Println("Listener error:", err)
		return
	}
	fmt.Printf("[%s] RPC server listening on %s\n", selfName, selfAddr)

	// Start heartbeat goroutine
	go heartbeat(selfName, selfAddr)

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
