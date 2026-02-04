## Fault-Tolerant Log Storage Engine with Raft Consensus
A highly available, disk-persistent log aggregation system designed for cloud-native environments. Built with Go and gRPC, it use the Raft consensus algorithm for fault-tolerant log replication and Serf for decentralized cluster membership, ensuring zero data loss and seamless scalability within Kubernetes. Features a secure-by-default architecture using mTLS and ACL-based authorization, providing monitoring through Open Telemetry and Zap.

| Feature | Description |
| :--- | :--- |
| 🏗️ **Distributed Consensus** | Leverages the **Raft algorithm** to ensure strict data consistency and fault-tolerant log replication across the cluster. |
| 📡 **Decentralized Discovery** | Utilizes **Serf (Gossip Protocol)** for automated peer-to-peer membership, allowing nodes to join/leave without manual configuration. |
| ⚡ **High-Performance RPC** | Built with **gRPC** and **Protobuf** for low-latency, strongly-typed communication between clients and storage nodes. |
| 🛡️ **Zero-Trust Security** | Production-grade security implementation featuring **mTLS** for encrypted transport and **ACLs** for granular authorization. |
| 📦 **Cloud-Native Deploy** | Fully containerized architecture with **Kubernetes** manifests and **Helm charts** for streamlined orchestration and scaling. |
| 💾 **Disk-Backed Storage** | Engineered for persistence with a custom file-backed storage engine that ensures data durability across restarts. |
| 🛠️ **Administrative CLI** | Includes a robust **Command Line Interface** for managing cluster state, inspecting logs, and monitoring health. |
