## Fault-Tolerant Log Storage Engine with Raft Consensus
A highly available, disk-persistent log aggregation system designed for cloud-native environments. Built with Go and gRPC, it use the Raft consensus algorithm for fault-tolerant log replication and Serf for decentralized cluster membership, ensuring zero data loss and seamless scalability within Kubernetes. Features a secure-by-default architecture using mTLS and ACL-based authorization, providing monitoring through Open Telemetry and Zap.
### Features:
- TLS/ACl authorization
- Raft based replication algorithm
-  Client based load balancer
-  Peer discovery using Serf
-  Simple CLI 
