package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/Hanningtone03/raft-consensus/internal/raft"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run cmd/node/main.go <node-id>")
		os.Exit(1)
	}

	id, _ := strconv.Atoi(os.Args[1])

	allNodes := map[int]int{
		1: 6001,
		2: 6002,
		3: 6003,
	}

	port := allNodes[id]
	var peers []int
	for nodeID := range allNodes {
		if nodeID != id {
			peers = append(peers, nodeID)
		}
	}

	node := raft.NewNode(id, port, allNodes, peers)
	node.Start()

	fmt.Printf("Node %d started on port %d, peers: %v\n", id, port, peers)
	fmt.Println("Watch the logs to see leader election happen automatically.")

	select {}
}
