# Table of Contents

- [Lab01: Distributed MapReduce in Go](#lab01-distributed-mapreduce-in-go)
- [Lab02: Key/Value Server and Distributed Lock](#lab02-keyvalue-server-and-distributed-lock)
- [Lab03: Replicated State Machine with Raft](#lab03-replicated-state-machine-with-raft)

---

# Lab01: Distributed MapReduce in Go

## Overview

This project implements a fault-tolerant, distributed MapReduce system in Go, inspired by the original MapReduce paper. It was developed as part of MIT 6.5840 coursework.

The system supports:

- Dynamic task assignment over RPC
- Crash fault tolerance (workers may fail and tasks will be reassigned)
- Parallel execution of map and reduce tasks
- Correct merging of output to match sequential reference

The implementation consists of a **Coordinator** (master) process and multiple **Worker** processes communicating over Go RPC on a shared filesystem.

## Fault Tolerance

- Coordinator reassigns tasks that timeout
- Workers handle RPC errors gracefully
- Coordinator and workers terminate cleanly when the job completes
- Uses atomic writes (via temporary files and renaming) to prevent partial file writes on crash

## File Layout

- [`src/`](src/)
    - [`mr/`](src/mr/)
        - [`coordinator.go`](src/mr/coordinator.go) – Coordinator implementation
        - [`worker.go`](src/mr/worker.go) – Worker implementation
        - [`rpc.go`](src/mr/rpc.go) – Shared RPC types

## Testing
![Lab01 result](images/Lab01.png)

---
# Lab02: Key/Value Server and Distributed Lock

## Overview

This project implements a **key/value server** for a single machine that:

- Ensures **at-most-once** execution for Put operations despite network failures.
- Guarantees **linearizability** of operations.
- Supports building a distributed **lock service** on top of it.

The final server is robust to dropped, delayed, or re-ordered network messages, while offering a simple, predictable interface to clients.

## Features Implemented
### ✅ Reliable KV Server
- Handles `Put` and `Get` RPCs from clients.
- Maintains an in-memory map: `map[key] = (value, version)`.
- Enforces conditional updates with version numbers to prevent lost updates.
- Supports concurrent client operations with **linearizability guarantees**.

### ✅ Client Retry Logic
- Clients automatically retry RPCs on timeout or dropped responses.
- Correctly distinguishes between:
    - `ErrVersion` (definite conflict)
    - `ErrMaybe` (possible previous success but reply lost)
- Ensures at-most-once semantics for Put under unreliable networks.

### ✅ Distributed Lock Built on KV Clerk
- Implements `Acquire` and `Release` methods.
- Multiple clients coordinate using the KV server to achieve mutual exclusion.
- Correctly handles `ErrMaybe` by retrying or detecting lock conflicts.

## File Layout

- [`src/`](src/)
    - [`kvsrv1/`](src/kvsrv1/)
        - [`client.go`](src/kvsrv1/client.go) – Clerk logic: Put/Get RPCs with retries, ErrMaybe handling
        - [`server.go`](src/kvsrv1/server.go) – Server logic: conditional Put/Get handlers
        - [`rpc/`](src/kvsrv1/rpc/)
            - [`rpc.go`](src/kvsrv1/rpc/rpc.go) – RPC types, error definitions
        - [`lock/`](src/kvsrv1/lock/)
            - [`lock.go`](src/kvsrv1/lock/lock.go) – Distributed lock built on Clerk.Put/Get
        - [`kvsrv_test.go`](src/kvsrv1/kvsrv_test.go) – Test suite

## Testing
![Lab02 result](images/Lab02.png)

---
# Lab03: Replicated State Machine with Raft

## Overview

This project implements Raft, a fault-tolerant replicated state machine protocol, in Go.

Raft ensures consistency across multiple servers by replicating a log of client commands. Even in the presence of crashes or network partitions, a majority of servers can continue making progress and maintain a coherent service state.

## Features Implemented

- Leader election with randomized timeouts
- Log replication and consistency
- Crash recovery via persistent state
- Log compaction through snapshotting
- Correct operation despite network partitions and reordering

This implementation closely follows the *extended Raft paper*, with particular attention to Figure 2, and serves as a solid foundation for building higher-level distributed services.


## File Layout

- [`src/`](src/)
    - [`raft1/`](src/raft1/)
        - [`raft.go`](src/raft1/raft.go) – Raft implementation
        - [`persister.go`](src/raft1/persister.go) – Persistent state abstraction


## Development Notes

This lab was completed in four parts, each adding essential features:

### Part 3A: Leader Election
- Randomized timeouts to trigger elections
- RequestVote RPCs
- AppendEntries RPCs as heartbeats
- Ensures a single leader exists and recovers leadership after failures

### Part 3B: Log Replication
- Client commands added to the replicated log
- AppendEntries RPCs carry new log entries
- Correct handling of log consistency and conflicts
- Commit index tracking and ApplyMsg delivery

### Part 3C: Persistence
- Persistent storage of Raft state (term, vote, log)
- Crash/restart recovery via serialization with labgob
- Integration with Persister abstraction

### Part 3D: Log Compaction (Snapshotting)
- Service-layer snapshot support via `Snapshot()`
- Log truncation before snapshot index
- InstallSnapshot RPC to synchronize lagging peers
- Persistent storage of both Raft state and snapshot data

## Testing

![Lab03 Result](images/Lab03.png)