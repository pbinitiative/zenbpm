# Table of contents
1. [Zen cluster](#zen-cluster)
   1. [Main cluster](#main-cluster)
      1. [Commands](#commands)
         1. [ClusterNodeChange](#clusternodechange)
         2. [ClusterNodePartitionChange](#clusternodepartitionchange)
         3. [ProcessDefinitionAllocation](#processdefinitionallocation)
      2. [Leader](#leader)
      3. [Followers](#followers)
   2. [Partition group clusters](#partition-group-clusters)
      1. [Leader](#leader-1)
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
4. [Authorization & Authentication](#authorization--authentication)
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

#### ProcessDefinitionAllocation
- decides the definition key and numeric version of a BPMN deployment once for the whole cluster, before the definition fans out to the partitions
- idempotent on (process id, content checksum) against the latest version of the process: a retried or concurrently repeated deployment gets the allocation that already exists, while older content deployed again becomes a new version (deploying A, then B, then A again leaves A as the latest version); a version tag pins content to one version, so a tagged deployment of the content already allocated under that tag is answered with that allocation (a retry after a partial failure completes it), and other content under a taken tag is rejected
- the deploying node first reads what every partition leader holds of the process (the latest version and every tagged version) and sends it along, so the allocation continues the version sequence of definitions deployed before allocations were replicated and never falls behind a partition; partition leaders are read rather than a local replica, which may lag behind
- every partition stores exactly the allocated (key, version), so concurrent deployments of one process id cannot map the same version to different content on different partitions
- the deploying node reads the partition leaders with a linearizable read (answered by the leader only, once every acknowledged write is applied), so the observation never lags a partition; an allocation whose recorded latest version turns out to be held by no partition while a partition holds another definition at that version is replaced by what the partitions hold, so a wrong observation cannot block a process for good
- a cluster restore replaces the partitions but not the main raft state, so its reconciliation resets the allocation registry (`ACTION_RESET`) from the process definitions the restored partitions hold. The reset carries the restore operation's fencing token and is applied only for the operation that owns the cluster while it reconciles; an allocation is refused while a restore gates the cluster, and carries the restore generation it observed the partitions under, so a deployment that observed them before a restore replaced them is refused too
- the command is new to the main raft log, and a node running a binary without it would stop when it applies the command (the FSM refuses unknown commands rather than diverge). Every node therefore announces the protocol version its binary implements (`NodeChange.protocol_version`, `state.CurrentProtocolVersion`) once the cluster state lists it, and the leader refuses to commit the command while any member of the raft configuration has not announced a version that includes it: a deployment during a rolling upgrade is answered with an error naming the member instead of crashing it, and succeeds once every member runs the new binary. A node recorded as shut down holds no announced version, whatever an announcement in flight says, so a node that comes back (possibly with another binary) announces again; a failed announcement is retried by the node with a backoff. Once the log holds the command, the cluster state records the protocol version it requires (`minProtocolVersion`) and a join by a binary below it is refused, since the new member would replay the command; admission and the commit of such commands are serialised on the leader so that a member can neither be checked before it joined nor join before the command it would not know is applied

##### Deployment retries and upgrade support

- Once `minProtocolVersion` includes allocation commands, a member going down or losing its announcement no longer blocks allocations. The all-member capability check is needed for initial activation only; join admission continues rejecting incompatible binaries. Downgrading an existing member's binary without removing its retained data is unsupported.
- Fan-out uses exactly the partition set captured by the observation, and every deployment RPC names its partition: the receiving node stores the definition on that partition only, which it must lead (otherwise the deployer retries against the current leader), and registers the definition-level subscriptions there when asked to. Each RPC also carries the observation's restore generation. Partition admission checks it and orders the entire definition/subscription operation against restore fences. The generation survives fence release and is initialized from the durable main-cluster restore state when a partition reopens. An obsolete request is refused as a conflict even after the restore completes; the deploying node then observes and allocates once more, so the caller's deployment completes against the restored partitions. A sender that predates cluster-wide allocation (no version) never observed anything and deploys under the partition's current generation.
- A failed deployment reserves its version tag even if no partition received it. Retry the **original content** to complete that allocation, or deploy corrected content with a **new tag**. There is intentionally no force-release operation: an in-flight request may still write the reserved identity. `alreadyExisted` describes allocation identity, not proof that partition persistence succeeded on a previous attempt; a retry may return HTTP 200 even if it is the first successful fanout.
- Supported stores are those written by released binaries (which hold no allocation registry) and by this revision of the command. **Retained raft logs and snapshots from earlier unmerged revisions of this branch (including `02040352` and `fec93a8a`) are not supported.** Their command shapes are gone: `ACTION_CONFIRM` and `observed_latest` are reserved field numbers the FSM refuses or ignores, and a snapshot recording a version tag as a bare number fails to decode, so a node pointed at such a data directory does not start. Keep a backup and recreate disposable development stores instead.

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
