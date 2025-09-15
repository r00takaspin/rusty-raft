# Rusty RAFT

## Run server
```bash
cargo run -- --id 1 --peers 127.0.0.1:8081,127.0.0.1:8082 --addr 127.0.0.1:8080
```

## Run client
```bash
telnet 8080
set x=5
exit
```

## Public api:
 * `set x={}` — sets value 
 * `state` — get node state
 * `quit, exit` — close connection

## Run tests
```bash
cargo test
```