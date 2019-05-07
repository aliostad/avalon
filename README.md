# avalon
An Implementation of [Raft](https://raft.github.io/raft.pdf) in .NET (including snapshots) - meant for the prime-time.

## Introduction to Raft
Raft is a consensus algorithm introduced back in 2014 as a simpler consensus algorithm compared to likes of [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)). Consensus algorithms are meant to solve the consistency problem of **a replicated log** across an unreliable distributed network providing high availability. The main advantage of Raft has been said to be its simplicity and ease of understanding and implementation, while maintaining comparable levels of consistency and availability.

Raft proposes a system whereby:

 - A cluster consisting typically of 5 nodes, only one of them being the *Leader*.
 - Every node can be either a *Leader*, *Follower* or *candidate*
 - Leader is elected through an election process after a timeout in receiving heartbeat from the current leader. A leadership is known by an integer *term* which monotonically increases. 
 - Leader is responsible for accepting commands to a *state machine* from the clients. Followers would normally pass on commands to the leader.
 - Every node is responsible for maintaining a persistent log of the commands, and an in-memory representation of the *state machine*. Every command is turned into a single *log entry*. Entries are identified by their index, starting from zero and monotonically increasing.
 - The state machine can be anything but for the purpose of illustration, consider the state machine as a dictionary where commands are key-values to be set against the dictionary (and perhaps null values for the deletion).
 - Leader is responsible for sending commands to follower nodes.
 - Raft can easily handle transient single node failures and can survive short failures of up to two nodes.

## Avalon - Core
Core part of Avalon is simply a generic and complete implementation of Raft and it can be used in a variety of settings.

Logs and state are persisted using [LMDB](https://lmdb.readthedocs.io/en/release/) which is a very fast local key/value database with additional NoSQL features. 

RPC and State-Machine implementations are left as generic and consumers are free to choose an RPC protocol and choice of state machine.

## Avalon - gRPC
This library uses [gRPC](https://grpc.io/) as the choice of protocol for the RPC.



