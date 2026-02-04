## Fault-Tolerant Log Storage Engine with Raft Consensus
A highly available, disk-persistent log aggregation system designed for cloud-native environments. Built with Go and gRPC, it use the Raft consensus algorithm for fault-tolerant log replication and Serf for decentralized cluster membership, ensuring zero data loss and seamless scalability within Kubernetes. Features a secure-by-default architecture using mTLS and ACL-based authorization, providing monitoring through Open Telemetry and Zap.

### Features
- **Distributed Consensus:** Utilizes hashicorp's Raft implementation to maintain strict data consistency and state machine replication across the cluster, ensuring no data loss during leader elections.
- **Decentralized Peer Discovery:** Leverages Serf (Gossip Protocol) for automated cluster membership, allowing nodes to dynamically discover each other without a centralized registry.
- **High-Performance gRPC:** Utilizes gRPC and Protocol Buffers for efficient, low-latency communication, log streaming, load balancing and strongly-typed API contracts between clients and storage nodes.
- **Production-Grade Security:** Built with a Zero-Trust approach, featuring mTLS for encrypted transport and ACL-based authorization to secure log access.
- **Cloud-Native Orchestration:** Fully optimized for Kubernetes with custom Helm charts, supporting seamless scaling and automated deployment lifecycles.
- **Persistent Log Engine**: Features a custom, disk-backed storage implementation designed for durability, ensuring logs are safely persisted across container restarts.
- **DevOps-Friendly CLI:** Includes a dedicated Command Line Interface to simplify cluster management, health monitoring, and log inspection.
