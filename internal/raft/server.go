package raft

import (
	"encoding/json"
	"net/http"
)

func StartServer(port int, node *Node) {
	mux := http.NewServeMux()
	state := node.State

	mux.HandleFunc("/requestVote", func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		json.NewDecoder(r.Body).Decode(&args)
		reply := state.HandleRequestVote(args)
		if reply.VoteGranted {
			node.ResetTimer()
		}
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/appendEntries", func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		json.NewDecoder(r.Body).Decode(&args)
		reply := state.HandleAppendEntries(args)
		if reply.Success {
			node.ResetTimer()
		}
		json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state.Lock()
		defer state.Unlock()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":           state.ID,
			"role":         state.Role.String(),
			"term":         state.CurrentTerm,
			"log_length":   len(state.Log),
			"commit_index": state.CommitIndex,
		})
	})

	mux.HandleFunc("/submit", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)
		if state.Role != Leader {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "not the leader"})
			return
		}
		entry := state.AppendCommand(body["command"])
		json.NewEncoder(w).Encode(entry)
	})

	go http.ListenAndServe(":"+portString(port), mux)
}

func portString(port int) string {
	digits := []byte{}
	if port == 0 {
		return "0"
	}
	for port > 0 {
		digits = append([]byte{byte('0' + port%10)}, digits...)
		port /= 10
	}
	return string(digits)
}
