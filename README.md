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

### Architecture Overview
#### 1. Log Layer
* Manages data persistence using a **segmented log architecture** for efficient storage management.
* Implements **automatic log rotation** by creating new segments once size thresholds are met.
* Enables high-speed retrieval through **indexed lookups** across multiple segment files.
* Simplifies data retention and lifecycle management by allowing old segments to be purged efficiently.

#### 2. Distribution Layer
* Orchestrates **cluster-wide consistency** using the Raft consensus algorithm for reliable state management.
* Manages **automatic leader election** and log replication to ensure the system remains fault-tolerant.
* Utilizes **Serf (Gossip Protocol)** for decentralized node discovery and real-time health monitoring.
* Prevents data loss during node failures or network partitions by requiring majority commitment.

#### 3. API & Transport Layer
* Serves as the system gateway using **gRPC** with a native **client-side load balancer** for optimal traffic routing.
* Enforces **Zero-Trust security** via mTLS encryption and granular ACL-based authorization.
* Integrated with **OpenTelemetry** for distributed tracing and **Zap** for high-performance structured logging.
* Provides a standardized interface for the **CLI** and external services to interact with the cluster state.
