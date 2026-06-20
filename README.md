![CI](https://github.com/Hanningtone03/raft-consensus/actions/workflows/ci.yml/badge.svg)

# Raft Consensus

An implementation of the Raft consensus algorithm in Go; leader election, log replication, heartbeats, and commit index advancement across a 3-node cluster.

## How it works

Each node starts as a follower with a randomized election timeout. If a follower doesn't hear from a leader in time, it becomes a candidate and requests votes from its peers. Once a candidate wins a majority, it becomes the leader and starts sending heartbeats. Commands submitted to the leader get appended to its log and replicated to followers on every heartbeat. Once a majority of nodes have replicated an entry, the leader advances its commit index.

## Project structure

```
cmd/node/
└── main.go
internal/raft/
├── state.go
├── election.go
├── log.go
├── node.go
└── server.go
internal/rpc/
└── client.go
```

## Running locally

Open three terminals:

```bash
go run cmd/node/main.go 1
go run cmd/node/main.go 2
go run cmd/node/main.go 3
```

Check status:

```bash
curl http://localhost:6001/status
```

Submit a command to whichever node is leader:

```bash
curl -X POST http://localhost:6002/submit -d '{"command":"set x = 1"}'
```

## What to expect

One node becomes leader within a second or two. Submitting a command to the leader replicates it to all followers, and the commit index advances once a majority acknowledges.

## Tech

- Go
- Plain HTTP for inter-node RPC
- No external dependencies
