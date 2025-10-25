package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

// startRPCServer launches the RPC listener for this node.
func (n *RaftNode) startRPCServer() {
	service := &RaftRPC{node: n}
	if err := rpc.RegisterName("RaftRPC", service); err != nil {
		log.Fatalf("[%s] RPC register failed: %v", n.ID, err)
	}
	addr := fmt.Sprintf(":%d", n.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[%s] RPC listen failed: %v", n.ID, err)
	}
	log.Printf("[%s] RPC listening on %s\n", n.ID, addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				log.Printf("[%s] accept error: %v", n.ID, err)
				continue
			}
		}
		go rpc.ServeConn(conn)
	}
}
