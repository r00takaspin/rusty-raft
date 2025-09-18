# Rusty RAFT

## Run server
```bash
cargo run -- --id 1 --peers 127.0.0.1:8081,127.0.0.1:8082 --addr 127.0.0.1:8080 --log-path /Users/voldemar/projects/rusty-raft/data/node_1.json
```

## Run client
```bash
telnet 8080
# set command
> {"type": "SetRequest", "data": {"command": "set 123"}}
<  {"ok":true}
# state command 
> {"type": "StateRequest", "data": {}}
< {"node_id":"1","term":5,"role":"Follower"}
# request vote
> {"type": "RequestVote", "data": {"node_id": "2", "term": 5, "last_log_term": 0, "last_log_index": 0}}
< {"ok":true,"term":5}
# append log message 
> {"type":"AppendLogRequest","data":{"leader_id":"2","leader_term":1,"prev_log_index":0,"prev_log_term":0,"leader_commit_index":1,"leader_log":[{"index":1,"term":1,"command":"test 1"}]}}
> {"ok": true, "term": 1}
# heartbeat
> {"type":"HeartbeatRequest","data":{"leader_id":"2","leader_term":1,"prev_log_index":1,"prev_log_term":1,"leader_commit_index":1}}
< {"ok": true, "term": 1}
```

## Run tests
```bash
cargo test
```