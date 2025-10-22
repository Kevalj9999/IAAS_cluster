package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func (n *RaftNode) startRPCServer() {
	rpcService := &RaftRPC{node: n}
	err := rpc.RegisterName("RaftRPC", rpcService)
	if err != nil {
		log.Fatalf("rpc register failed: %v", err)
	}
	addr := fmt.Sprintf(":%d", n.Port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen failed: %v", err)
	}
	log.Printf("[%s] RPC server listening on %s\n", n.ID, addr)
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}
		go rpc.ServeConn(conn)
	}
}
