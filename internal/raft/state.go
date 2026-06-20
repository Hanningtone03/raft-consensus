package raft

import "sync"

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	}
	return "UNKNOWN"
}

type LogEntry struct {
	Term    int
	Index   int
	Command string
}

type State struct {
	mu sync.Mutex

	ID          int
	Role        Role
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	CommitIndex int
	LastApplied int

	Peers []int
}

func NewState(id int, peers []int) *State {
	return &State{
		ID:          id,
		Role:        Follower,
		CurrentTerm: 0,
		VotedFor:    -1,
		Log:         []LogEntry{},
		CommitIndex: 0,
		LastApplied: 0,
		Peers:       peers,
	}
}

func (s *State) Lock()   { s.mu.Lock() }
func (s *State) Unlock() { s.mu.Unlock() }

func (s *State) LastLogIndex() int {
	if len(s.Log) == 0 {
		return 0
	}
	return s.Log[len(s.Log)-1].Index
}

func (s *State) LastLogTerm() int {
	if len(s.Log) == 0 {
		return 0
	}
	return s.Log[len(s.Log)-1].Term
}

func (s *State) BecomeFollower(term int) {
	s.Role = Follower
	s.CurrentTerm = term
	s.VotedFor = -1
}

func (s *State) BecomeCandidate() int {
	s.Role = Candidate
	s.CurrentTerm++
	s.VotedFor = s.ID
	return s.CurrentTerm
}

func (s *State) BecomeLeader() {
	s.Role = Leader
}
