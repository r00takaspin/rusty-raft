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
< {"success":true,"term":5}
--  

```

## Run tests
```bash
cargo test
```