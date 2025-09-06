# Table of contents
1. [Zen cluster](#zen-cluster)
   1. [Main cluster](#main-cluster)
      1. [Commands](#commands)
         1. [ClusterNodeChange](#clusternodechange)
         2. [ClusterNodePartitionChange](#clusternodepartitionchange)
      2. [Leader](#leader)
      3. [Followers](#followers)
   2. [Partition groups](#partition-groups)
      1. [Leader](#leader)
      2. [Follower](#follower)
2. [Zen node](#zen-node)
   1. [Behaviour](#behaviour)
      1. [Start](#start)
      2. [Normal operation](#normal-operation)
      3. [Shutdown](#shutdown)
   2. [Private gRPC communication](#private-grpc-communication)
   3. [Public gRPC](#public-grpc)
   4. [Public REST API](#public-rest-api)
   5. [System REST API](#system-rest-api)
3. [Networking](#networking)
4. [Authorization & Authentication](#authorization-&-authentication)
5. [Observability](#observability)

# Zen cluster
Zen cluster is a RAFT cluster composed of:  
 - Main cluster
 - Partition group clusters

## Main cluster
Main cluster has a role of a controller of Partition groups.  
 - it decides which node will **join** which Partition group.
 - invokes **rebalance of the partitions** when cluster state becomes invalid. (ex. Partition group has too few members for too long)
 - supports **zones**. For case of high availability deployment support zones so that each node can be marked with a zone it is in. Zone is used during rebalance to ensure that not all members of Partition group are in the same zone.

### Commands
This is the list of protobuf raft log commands. Each node needs to come to the same cluster state after consuming Raft log.
See source: [zencommand.proto](./command/proto/zencommand.proto)

#### ClusterNodeChange
- cluster state changes
- internal gRPC address changes
- cluster role changes

#### ClusterNodePartitionChange
- partition state changes
- partition role changes

### Leader
Main cluster leader is one node responsible for the state of whole Zen cluster. It manages:
 - memberships of the Partition groups
 - controls backup and restore procedures
 - distributes configuration updates
 - monitors the cluster state
Due to leader handling many tasks around cluster management it **can** be **configured to not be a member** of any Partition groups.  
This helps to keep leaders compute resources allocated to cluster management and not be shared with BPMN & DMN engines.
In **simplified** deployments, the leader can also be a member of Partition, which will allow it to run BPMN & DMN engines.

### Followers
Followers are the main workforce of Zen cluster. After a node joins into a cluster which has already elected its leader and restores its local cluster log, leader will write a **NodePartitionChange** command which changes the state of the node in the cluster.  
After receiving this change, follower joins the Partition group and depending on the state of the group it either becomes:
 - Partition leader 
 - Partition follower

## Partition group clusters
Partition groups are separate RqLite Raft clusters that are **controlled by Main cluster**. 
When a node is part of the Partition group and is:
 - leader in that Partition group it will: 
   - start its BPMN & DMN engines and start processing requests and instances for that concrete partition.
   - listen on the private API port for commands on the partition
 - follower in that Partition group will:
   - listen on the private API port for queries on the partition

### Leader
Has to notify leader of the cluster that it became a partition group leader (so that the cluster leader can update cluster state).
Acts on observations of partition cluster and relays the information to the cluster leader (node became unresponsive, shutdown, ...).
Performs write/read operations into the database.

### Follower
Consumes raft log and updates the database state based on leader writes.
Provides an API to read information from partition.

# Zen node
Zen node is a Zen application running either by itself in simplified configuration or as a part of the Zen cluster.
When a node receives a query through the Public API it evaluates if its a command or query request and which Partition group needs to process it. Queries are processed by followers and Commands are executed by leaders.
If the current node cannot handle the request, the request is proxied to the node that can handle it.

## Behaviour

### Start
When a node is starting it first checks if it has already been part of the raft cluster. If so it will start communicating with member nodes and catch up to the latest offset of raft log.
If a node is started from clean start it will read the application configuration and try to establish new raft cluster.

### Normal operation
Watches for changes in cluster state object (performed by raft writes) and processes them based on its role.
BPMN engine is active on a node only in case that it is a leader of a partition.

### Shutdown

## Private gRPC communication
Internal communication between nodes.
- Notify - sent by a node to peers when it is ready for bootstrapping
- Join - joins a node to raft cluster (recipient is leader)
- ClusterBackup - request to start a cluster backup (recipient is leader)
- ClusterRestore - request to start a cluster restore (recipient is leader)
- ConfigurationUpdate - configuration update request (recipient is leader)
- AssignPartition - request to join partition group (recipient is follower)
- UnassignPartition - request to leave partition group (recipient is member of partition group)
- PartitionBackup - request to back up partition (recipient is leader of partition group)
- PartitionRestore - request to restore partition (recipient is leader of partition group)

- NodeCommand - updates from nodes propagated to raft log (recipient is leader)

## Public gRPC 
Public gRPC endpoint that exposes jobs handling endpoints for better performance compared to REST API.

## Public REST API
Public REST API provides similar capabilities to Public gRPC API only through the REST API.

## System REST API
System REST API contains:
 - OTEL prometheus metrics exporter endpoint
 - health check endpoint
 - readiness check endpoint

# Networking
Application exposes 3 ports:
- public REST API that should be exposed to outside world to interact with the cluster and bpmn engine
- public gRPC API that should be exposed to outside world to interact with the cluster and bpmn engine
- private API used for:
  - internal zen cluster communication between nodes
  - internal zen raft communication 
  - internal rqlite cluster communication between nodes
  - internal rqlite raft communication 
  To prevent the need to manage multiple internal ports the port is wrapped in a multiplexer that handles the communication recipient based on the first byte sent through the TCP connection. Details can be found in `internal/cluster/network/network.go`

When executing an operation against data in partition any node can receive a network call (even those that are not part of that partition). Based on the id of the object (snowflake id) we determine the partition leader to which the call should be proxied (proxied call goes through the internal gRPC API). This means that any node can receive any request and internal handler will make sure that it is received by a correct node that can process the request.

# Authorization & Authentication

# Observability
Application provides OpenTelementry support through:
 - Prometheus metrics exporter
 - application traces. Configurable to multiple levels:
   - Public APIs
   - engine execution
     - external workers
     - full
