package raft

import (
	"log"
	"math/rand"
	"time"

	"github.com/Hanningtone03/raft-consensus/internal/rpc"
)

type Node struct {
	State     *State
	Port      int
	PeerPorts map[int]int
	resetChan chan bool
}

func NewNode(id int, port int, peerPorts map[int]int, peers []int) *Node {
	return &Node{
		State:     NewState(id, peers),
		Port:      port,
		PeerPorts: peerPorts,
		resetChan: make(chan bool, 1),
	}
}

func (n *Node) Start() {
	StartServer(n.Port, n)
	go n.runElectionTimer()
}

func (n *Node) runElectionTimer() {
	for {
		timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
		timer := time.NewTimer(timeout)

		select {
		case <-n.resetChan:
			timer.Stop()
			continue
		case <-timer.C:
			n.State.Lock()
			role := n.State.Role
			n.State.Unlock()

			if role != Leader {
				n.startElection()
			}
		}
	}
}

func (n *Node) ResetTimer() {
	select {
	case n.resetChan <- true:
	default:
	}
}

func (n *Node) startElection() {
	args := n.State.PrepareVoteRequest()
	log.Printf("[node %d] starting election for term %d", n.State.ID, args.Term)

	votes := 1
	totalNodes := len(n.State.Peers) + 1
	majority := totalNodes/2 + 1

	for _, peerID := range n.State.Peers {
		port := n.PeerPorts[peerID]
		var reply RequestVoteReply
		err := rpc.Call("127.0.0.1", port, "requestVote", args, &reply)
		if err != nil {
			continue
		}
		if reply.VoteGranted {
			votes++
		}
		if reply.Term > n.State.CurrentTerm {
			n.State.Lock()
			n.State.BecomeFollower(reply.Term)
			n.State.Unlock()
			return
		}
	}

	n.State.Lock()
	stillCandidate := n.State.Role == Candidate && n.State.CurrentTerm == args.Term
	n.State.Unlock()

	if stillCandidate && votes >= majority {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
	n.State.Lock()
	n.State.BecomeLeader()
	term := n.State.CurrentTerm
	n.State.Unlock()

	log.Printf("[node %d] became LEADER for term %d with majority votes", n.State.ID, term)

	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.State.Lock()
		if n.State.Role != Leader {
			n.State.Unlock()
			return
		}
		term := n.State.CurrentTerm
		logCopy := append([]LogEntry{}, n.State.Log...)
		leaderLogLength := len(n.State.Log)
		n.State.Unlock()

		ackCount := 1

		for _, peerID := range n.State.Peers {
			port := n.PeerPorts[peerID]
			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.State.ID,
				Entries:      logCopy,
				LeaderCommit: n.State.CommitIndex,
			}
			var reply AppendEntriesReply
			err := rpc.Call("127.0.0.1", port, "appendEntries", args, &reply)
			if err != nil {
				continue
			}

			if reply.Success {
				ackCount++
			}

			if reply.Term > term {
				n.State.Lock()
				n.State.BecomeFollower(reply.Term)
				n.State.Unlock()
				return
			}
		}

		majority := (len(n.State.Peers)+1)/2 + 1
		if ackCount >= majority && leaderLogLength > 0 {
			n.State.Lock()
			if leaderLogLength > n.State.CommitIndex {
				n.State.CommitIndex = leaderLogLength
				log.Printf("[node %d] commit index advanced to %d", n.State.ID, n.State.CommitIndex)
			}
			n.State.Unlock()
		}
	}
}
