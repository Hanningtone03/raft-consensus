package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (s *State) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	s.Lock()
	defer s.Unlock()

	reply := AppendEntriesReply{Term: s.CurrentTerm, Success: false}

	if args.Term < s.CurrentTerm {
		return reply
	}

	if args.Term > s.CurrentTerm {
		s.BecomeFollower(args.Term)
	} else if s.Role != Follower {
		s.Role = Follower
	}

	reply.Term = s.CurrentTerm

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(s.Log) {
			return reply
		}
		if args.PrevLogIndex > 0 && s.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return reply
		}
	}

	for _, entry := range args.Entries {
		if entry.Index <= len(s.Log) {
			s.Log[entry.Index-1] = entry
		} else {
			s.Log = append(s.Log, entry)
		}
	}

	if args.LeaderCommit > s.CommitIndex {
		if args.LeaderCommit < len(s.Log) {
			s.CommitIndex = args.LeaderCommit
		} else {
			s.CommitIndex = len(s.Log)
		}
	}

	reply.Success = true
	return reply
}

func (s *State) AppendCommand(command string) LogEntry {
	s.Lock()
	defer s.Unlock()

	entry := LogEntry{
		Term:    s.CurrentTerm,
		Index:   len(s.Log) + 1,
		Command: command,
	}
	s.Log = append(s.Log, entry)
	return entry
}
