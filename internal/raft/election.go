package raft

import "log"

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (s *State) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	s.Lock()
	defer s.Unlock()

	reply := RequestVoteReply{Term: s.CurrentTerm, VoteGranted: false}

	if args.Term < s.CurrentTerm {
		return reply
	}

	if args.Term > s.CurrentTerm {
		s.BecomeFollower(args.Term)
	}

	reply.Term = s.CurrentTerm

	logOk := args.LastLogTerm > s.LastLogTerm() ||
		(args.LastLogTerm == s.LastLogTerm() && args.LastLogIndex >= s.LastLogIndex())

	if (s.VotedFor == -1 || s.VotedFor == args.CandidateID) && logOk {
		s.VotedFor = args.CandidateID
		reply.VoteGranted = true
		log.Printf("[node %d] voted for node %d in term %d", s.ID, args.CandidateID, s.CurrentTerm)
	}

	return reply
}

func (s *State) PrepareVoteRequest() RequestVoteArgs {
	s.Lock()
	defer s.Unlock()

	term := s.BecomeCandidate()
	return RequestVoteArgs{
		Term:         term,
		CandidateID:  s.ID,
		LastLogIndex: s.LastLogIndex(),
		LastLogTerm:  s.LastLogTerm(),
	}
}