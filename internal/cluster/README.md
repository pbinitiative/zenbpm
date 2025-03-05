# Table of contents
1. [ZenBPM cluster](#zenbpm-cluster)
   1. [Main cluster](#main-cluster)
      1. [Messages](#messages)
         1. [Cluster messages:](#cluster-messages:)
         2. [Partition messages:](#partition-messages:)
      2. [Leader](#leader)
      3. [Followers](#followers)
   2. [Partition groups](#partition-groups)
2. [ZenBPM node](#zenbpm-node)
   1. [Private GRPC communication](#private-grpc-communication)
   2. [Public GRPC](#public-grpc)
   3. [Public REST API](#public-rest-api)
   4. [System REST API](#system-rest-api)
3. [Authorization & Authentication](#authorization-&-authentication)
4. [Observability](#observability)

# ZenBPM cluster
ZenBPM cluster is a RAFT cluster composed from:  
 - Main cluster
 - Partition groups

## Main cluster
Main cluster has a role of a controller of Partition groups.  
 - it decides which node will **join** which Partition group.
 - invokes **rebalance of the partitions** when cluster state becomes invalid. (ex. Partition group has too few members for too long)
 - supports **zones**. For case of high availability deployment support zones so that each node can be marked with a zone it is in. Zone is used during rebalance to ensure that not all members of Partition group are in the same zone.

### Messages
This is the list of protobuf raft log messages. Each node needs to come to the same cluster state after consuming Raft log.

#### Cluster messages:
- NodeStateChange: 
  - node leaves Main cluster
  - node becomes unavailable
  - node joins into the Main cluster
- ClusterBackupStart:
  - backup of the cluster starts
- ClusterBackupEnd:
  - backup of the cluster ends
- ClusterRestoreStart:
  - restore of the cluster starts
- ClusterRestoreEnd:
  - restore of the cluster ends
- ConfigurationUpdate:
  - update the configuration of the cluster
- ConfigurationUpdateStart:
  - cluster started to update its configuration
- ConfigurationUpdateEnd:
  - update of the configuration of the cluster is finished

#### Partition messages:  
- PartitionStateChange:
  - node leaves Partition raft group
  - node becomes unavailable
  - node joins into the partition group
- PartitionAssignement:
  - message from leader with command to join a specific Partition group
- PartitionUnassignement:
  - message from leader with command to leave a specific Partition group
- PartitionBackupStart:
  - backup of the partition starts
- PartitionBackupEnd:
  - backup of the partition ends
- PartitionRestoreStart:
  - restore of the partition starts
- PartitionRestoreEnd:
  - restore of the partition ends


### Leader
Main cluster leader is one node responsible for the state of whole ZenBPM cluster. It manages:
 - memberships of the Partition groups
 - controls backup and restore procedures
 - distributes configuration updates
 - monitors the cluster state
Due to leader handling many tasks around cluster management it can be **configured to not be a member** of Partition groups.  
This helps to keep leaders compute resources allocated to cluster management and not be shared with BPMN & DMN engines.
In **simplified** deployments leader can also be a member of Partition which will allow him to run BPMN & DMN engines.

### Followers
Followers are the main workforce of ZenBPM cluster. After a node joins into a cluster which has already elected its leader and restores its local cluster log, leader will send a **PartitionAssignement** message/s with instructions on how to join Partition group.  
After receiving this message follower joins the Partition group and depending on the state of the group it either becomes:
 - Partition leader 
 - Partition follower

## Partition groups
Partition groups are separate RqLite Raft clusters that are **controlled by Main cluster**. 
When a node is part of the Partition group and is:
 - leader in that Partition group it will: 
   - listen on the public API ports for commands on the partition
   - start its BPMN & DMN engines and start processing requests and instances for that concrete partition.
 - follower in that Partition group it will:
   - listen on the public API ports for queries on the partition

# ZenBPM node
ZenBPM node is a ZenBPM application running either by itself in simplified configuration or as a part of the ZenBPM cluster.
When a node receives a query through the Public API it evaluates if its a command or query request and which Partition group needs to process it. Queries are processed by followers and Commands are executed by leaders.
If the current node cannot handle the request, the request is proxied to the node that can handle it.

## Private GRPC communication
Internal communication between nodes.

## Public GRPC 
Public GRPC endpoint provides compatibility with latest [Camunda GRPC API](https://github.com/camunda/camunda/blob/8.5.15/zeebe/gateway-protocol/src/main/proto/gateway.proto) and extends it with additional ZenBPM capabilities.

## Public REST API
Public REST API provides similar capabilities to Public GRPC API only through the REST API.

## System REST API
System REST API contains:
 - OTEL prometheus metrics exporter endpoint
 - health check endpoint
 - readiness check endpoint

# Authorization & Authentication

# Observability
Application provides OpenTelementry support through:
 - Prometheus metrics exporter
 - application traces. Configurable to multiple levels:
   - Public APIs
   - engine execution
     - external workers
     - full
