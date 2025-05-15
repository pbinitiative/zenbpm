package cluster

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rqlite/rqlite/v8/auto/backup"
	"github.com/rqlite/rqlite/v8/auto/restore"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/command/proto"
	httpd "github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
)

const (
	observerChanLen = 100
)

// ZenPartitionNode is part of the rqlite raft cluster (partition of the main cluster)
type ZenPartitionNode struct {
	partitionId     uint32
	config          *config.RqLite
	store           *store.Store
	rqliteDB        *RqLiteDB
	credentialStore *auth.CredentialsStore
	clusterClient   *cluster.Client
	clusterService  *cluster.Service
	statusMu        sync.Mutex
	statuses        map[string]httpd.StatusReporter
	logger          hclog.Logger

	engine *bpmn.Engine

	// Raft changes observer
	observer      *raft.Observer
	observerChan  chan raft.Observation
	observerClose chan struct{}
	observerDone  chan struct{}

	// callback functions that notify node about changes in the partition cluster
	stateChangeCallbacks PartitionChangesCallbacks
}

type PartitionChangesCallbacks struct {
	// new node has joined the partition raft cluster
	addNewNode func(raft.Server) error
	// node has been removed from partition raft cluster
	shutdownNode func(raft.ServerID) error
	// leader changed
	leaderChange func(raft.ServerID) error
	// node needs to be removed according to reap settings
	removeNode func(id string) error
	// partition node has become available
	resumeNode func(id string) error
}

func StartZenPartitionNode(ctx context.Context, mux *tcp.Mux, cfg *config.RqLite, partition uint32, callbacks PartitionChangesCallbacks) (*ZenPartitionNode, error) {
	zpn := ZenPartitionNode{
		config:               cfg,
		statuses:             map[string]httpd.StatusReporter{},
		partitionId:          partition,
		logger:               hclog.Default().Named(fmt.Sprintf("zen-partition-node-%d", partition)),
		stateChangeCallbacks: callbacks,
	}
	zpn.logger.Info(fmt.Sprintf("Starting partition %d node", partition))

	// Raft internode layer
	raftLn := network.NewRqLiteRaftListener(partition, mux)
	raftDialer, err := network.NewRqLiteRaftDialer(partition, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
		cfg.NodeVerifyServerName, cfg.NoNodeVerify)
	if err != nil {
		return nil, fmt.Errorf("failed to create RqLite Raft dialer: %w", err)
	}
	raftTn := tcp.NewLayer(raftLn, raftDialer)

	// Create the store.
	str, err := zpn.createStore(cfg, raftTn, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	zpn.store = str

	zpn.rqliteDB, err = NewRqLiteDB(
		zpn.store,
		zpn.partitionId,
		hclog.Default().Named(fmt.Sprintf("zen-partition-sql-%d", partition)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create rqLiteDB for partition %d: %w", partition, err)
	}

	// Install the auto-restore data, if necessary.
	if cfg.AutoRestoreFile != "" {
		hd, err := store.HasData(str.Path())
		if err != nil {
			return nil, fmt.Errorf("failed to check for existing data: %w", err)
		}
		if hd {
			zpn.logger.Info(fmt.Sprintf("auto-restore requested, but data already exists in %s, skipping", str.Path()))
		} else {
			zpn.logger.Info("auto-restore requested, initiating download")
			start := time.Now()
			path, errOK, err := restore.DownloadFile(ctx, cfg.AutoRestoreFile)
			if err != nil {
				var b strings.Builder
				b.WriteString(fmt.Sprintf("failed to download auto-restore file: %s", err.Error()))
				if errOK {
					b.WriteString(", continuing with node startup anyway")
					zpn.logger.Info(b.String())
				} else {
					s := b.String()
					return nil, fmt.Errorf(s, nil)
				}
			} else {
				zpn.logger.Info(fmt.Sprintf("auto-restore file downloaded in %s", time.Since(start)))
				if err := str.SetRestorePath(path); err != nil {
					return nil, fmt.Errorf("failed to preload auto-restore data: %w", err)
				}
			}
		}
	}

	// Get any credential store.
	credStr, err := zpn.createCredentialStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get credential store: %w", err)
	}
	zpn.credentialStore = credStr

	// Create cluster service now, so nodes will be able to learn information about each other.
	clstrServ, err := zpn.createClusterService(cfg, network.NewRqLiteClusterListener(partition, mux), str, str, credStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster service: %w", err)
	}
	zpn.clusterService = clstrServ
	zpn.logger.Info(fmt.Sprintf("cluster TCP mux Listener registered with byte header %d", network.GetPartitionClusterHeaderByte(partition)))

	// Create the HTTP service.
	//
	// We want to start the HTTP server as soon as possible, so the node is responsive and external
	// systems can see that it's running. We still have to open the Store though, so the node won't
	// be able to do much until that happens however.
	clstrClient, err := zpn.createClusterClient(cfg, clstrServ, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster client: %w", err)
	}
	zpn.clusterClient = clstrClient

	// Now, open store. How long this takes does depend on how much data is being stored by rqlite.
	if err := str.Open(); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}

	observerChan := make(chan raft.Observation, observerChanLen)
	zpn.observer = raft.NewObserver(observerChan, true, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		_, isResumedHeartBeat := o.Data.(raft.ResumedHeartbeatObservation)
		_, isPeerChange := o.Data.(raft.PeerObservation)
		return isLeaderChange || isFailedHeartBeat || isResumedHeartBeat || isPeerChange
	})
	str.RegisterObserver(zpn.observer)
	zpn.observerClose, zpn.observerDone = zpn.observe()

	// Register remaining status providers.
	zpn.registerStatus("cluster", clstrServ)
	zpn.registerStatus("network", tcp.NetworkReporter{})

	// Create the cluster!
	nodes, err := str.Nodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes %w", err)
	}
	if err := zpn.createPartitionCluster(ctx, cfg, len(nodes) > 0); err != nil {
		return nil, fmt.Errorf("clustering failure: %w", err)
	}

	// Start any requested auto-backups
	backupSrv, err := zpn.startAutoBackups(ctx, cfg, str)
	if err != nil {
		return nil, fmt.Errorf("failed to start auto-ackups: %w", err)
	}
	if backupSrv != nil {
		zpn.registerStatus("auto_backups", backupSrv)
	}
	return &zpn, nil
}

func (zpn *ZenPartitionNode) IsLeader(ctx context.Context) bool {
	return zpn.store.IsLeader()
}

func (zpn *ZenPartitionNode) Role() zproto.Role {
	if zpn.store.IsLeader() {
		return zproto.Role_ROLE_TYPE_LEADER
	}
	return zproto.Role_ROLE_TYPE_FOLLOWER
}

// Execute an SQL statement on rqlite partition node
func (zpn *ZenPartitionNode) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return zpn.store.Execute(req)
}

// Run an SQL query on rqlite partition node
func (zpn *ZenPartitionNode) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	return zpn.store.Query(req)
}

func (zpn *ZenPartitionNode) WaitForLeader(timeout time.Duration) (string, error) {
	return zpn.store.WaitForLeader(timeout)
}

func (zpn *ZenPartitionNode) Stats() (map[string]interface{}, error) {
	return zpn.clusterClient.Stats()
}

func (zpn *ZenPartitionNode) Stop() error {
	// stop the engine if present
	if zpn.engine != nil {
		zpn.engine.Stop()
	}
	zpn.clusterService.Close()
	if zpn.config.RaftClusterRemoveOnShutdown {
		remover := cluster.NewRemover(zpn.clusterClient, 1*time.Second, zpn.store)
		remover.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, zpn.config.JoinAs))
		zpn.logger.Info("initiating removal of this node from cluster before shutdown")
		if err := remover.Do(zpn.config.NodeID, true); err != nil {
			return fmt.Errorf("failed to remove this node from cluster before shutdown: %w", err)
		}
		zpn.logger.Info("removed this node successfully from cluster before shutdown")
	}

	if zpn.config.RaftStepdownOnShutdown {
		if zpn.store.IsLeader() {
			// Don't log a confusing message if (probably) not Leader
			zpn.logger.Info("stepping down as Leader before shutdown")
		}
		// Perform a stepdown, ignore any errors.
		zpn.store.Stepdown(true)
	}

	if err := zpn.store.Close(true); err != nil {
		zpn.logger.Info(fmt.Sprintf("failed to close store: %s", err.Error()))
	}
	zpn.logger.Info("rqlite server stopped")
	return nil
}

func (zpn *ZenPartitionNode) registerStatus(key string, stat httpd.StatusReporter) error {
	zpn.statusMu.Lock()
	defer zpn.statusMu.Unlock()

	if _, ok := zpn.statuses[key]; ok {
		return fmt.Errorf("status already registered with key %s", key)
	}
	zpn.statuses[key] = stat

	return nil
}

func (zpn *ZenPartitionNode) startAutoBackups(ctx context.Context, cfg *config.RqLite, str *store.Store) (*backup.Uploader, error) {
	if cfg.AutoBackupFile == "" {
		return nil, nil
	}

	b, err := backup.ReadConfigFile(cfg.AutoBackupFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read auto-backup file: %s", err.Error())
	}

	uCfg, s3cfg, err := backup.Unmarshal(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse auto-backup file: %s", err.Error())
	}
	provider := store.NewProvider(str, uCfg.Vacuum, !uCfg.NoCompress)
	sc, err := aws.NewS3Client(s3cfg.Endpoint, s3cfg.Region, s3cfg.AccessKeyID, s3cfg.SecretAccessKey,
		s3cfg.Bucket, s3cfg.Path, &aws.S3ClientOpts{
			ForcePathStyle: s3cfg.ForcePathStyle,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to create aws S3 client: %s", err.Error())
	}
	u := backup.NewUploader(sc, provider, time.Duration(uCfg.Interval))
	u.Start(ctx, str.IsLeader)
	return u, nil
}

func (zpn *ZenPartitionNode) createStore(cfg *config.RqLite, ln *tcp.Layer, partition uint32) (*store.Store, error) {
	dbConf := store.NewDBConfig()
	dbConf.OnDiskPath = cfg.OnDiskPath
	dbConf.FKConstraints = cfg.FKConstraints

	str := store.New(ln, &store.Config{
		DBConf: dbConf,
		Dir:    cfg.DataPath,
		ID:     cfg.NodeID,
		Logger: hclog.Default().
			Named(fmt.Sprintf("rqlite-partition-store-%d", partition)).
			StandardLogger(&hclog.StandardLoggerOptions{
				ForceLevel: hclog.Default().GetLevel(),
			}),
	})

	// Set optional parameters on store.
	str.RaftLogLevel = cfg.RaftLogLevel
	str.ShutdownOnRemove = cfg.RaftShutdownOnRemove
	str.SnapshotThreshold = cfg.RaftSnapThreshold
	str.SnapshotThresholdWALSize = cfg.RaftSnapThresholdWALSize
	str.SnapshotInterval = cfg.RaftSnapInterval
	str.LeaderLeaseTimeout = cfg.RaftLeaderLeaseTimeout
	str.HeartbeatTimeout = cfg.RaftHeartbeatTimeout
	str.ElectionTimeout = cfg.RaftElectionTimeout
	str.ApplyTimeout = cfg.RaftApplyTimeout
	str.BootstrapExpect = cfg.BootstrapExpect
	str.ReapTimeout = cfg.RaftReapNodeTimeout
	str.ReapReadOnlyTimeout = cfg.RaftReapReadOnlyNodeTimeout
	str.AutoVacInterval = cfg.AutoVacInterval

	if store.IsNewNode(cfg.DataPath) {
		zpn.logger.Info(fmt.Sprintf("no preexisting node state detected in %s, node may be bootstrapping", cfg.DataPath))
	} else {
		zpn.logger.Info(fmt.Sprintf("preexisting node state detected in %s", cfg.DataPath))
	}

	return str, nil
}

func (zpn *ZenPartitionNode) observe() (closeCh, doneCh chan struct{}) {
	closeCh = make(chan struct{})
	doneCh = make(chan struct{})

	go func() {
		defer close(doneCh)
		for {
			select {
			case o := <-zpn.observerChan:
				switch signal := o.Data.(type) {
				case raft.ResumedHeartbeatObservation:
					if zpn.stateChangeCallbacks.resumeNode == nil {
						break
					}
					if err := zpn.stateChangeCallbacks.resumeNode(string(signal.PeerID)); err == nil {
						zpn.logger.Info(fmt.Sprintf("partition node %s was resumed in the state", signal.PeerID))
					}
				case raft.FailedHeartbeatObservation:
					nodes, err := zpn.store.Nodes()
					if err != nil {
						zpn.logger.Error(fmt.Sprintf("failed to get partition nodes configuration during reap check: %s", err.Error()))
					}
					servers := store.Servers(nodes)
					id := string(signal.PeerID)
					dur := time.Since(signal.LastContact)

					isReadOnly, found := servers.IsReadOnly(id)
					if !found {
						zpn.logger.Error(fmt.Sprintf("partition node %s (failing heartbeat) is not present in configuration", id))
						break
					}

					if zpn.stateChangeCallbacks.shutdownNode != nil && zpn.config.RaftHeartbeatShutdownTimeout > 0 && dur > zpn.config.RaftHeartbeatShutdownTimeout {
						if err = zpn.stateChangeCallbacks.shutdownNode(signal.PeerID); err == nil {
							zpn.logger.Info(fmt.Sprintf("partition node %s was shutdown in the state", signal.PeerID))
						}
					}

					if zpn.stateChangeCallbacks.removeNode != nil && ((isReadOnly && zpn.config.RaftReapReadOnlyNodeTimeout > 0 && dur > zpn.config.RaftReapReadOnlyNodeTimeout) ||
						(!isReadOnly && zpn.config.RaftReapNodeTimeout > 0 && dur > zpn.config.RaftReapNodeTimeout)) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := zpn.stateChangeCallbacks.removeNode(id); err != nil {
							zpn.logger.Error(fmt.Sprintf("failed to reap partition node %s %s: %s", pn, id, err.Error()))
						} else {
							zpn.logger.Info(fmt.Sprintf("successfully reaped partition node %s %s", pn, id))
						}
					}
				case raft.LeaderObservation:
					if zpn.stateChangeCallbacks.leaderChange == nil {
						break
					}
					_ = zpn.stateChangeCallbacks.leaderChange(signal.LeaderID)
				case raft.PeerObservation:
					// PeerObservation is invoked only when the raft replication goroutine is started/stoped
					var err error
					if signal.Removed && zpn.stateChangeCallbacks.shutdownNode != nil {
						if err = zpn.stateChangeCallbacks.shutdownNode(signal.Peer.ID); err == nil {
							zpn.logger.Info(fmt.Sprintf("partition node %s was shutdown in the state", signal.Peer.ID))
						}
					} else if !signal.Removed && zpn.stateChangeCallbacks.addNewNode != nil {
						if err = zpn.stateChangeCallbacks.addNewNode(signal.Peer); err == nil {
							zpn.logger.Debug(fmt.Sprintf("partition node %s was updated in the state", signal.Peer.ID))
						}
					}
					if err != nil {
						zpn.logger.Error(fmt.Sprintf("failed to update peer observation in partition %d: %s", zpn.partitionId, err))
					}
				}
			case <-closeCh:
				return
			}
		}
	}()
	return closeCh, doneCh
}

func (zpn *ZenPartitionNode) createCredentialStore(cfg *config.RqLite) (*auth.CredentialsStore, error) {
	if cfg.AuthFile == "" {
		return nil, nil
	}
	return auth.NewCredentialsStoreFromFile(cfg.AuthFile)
}

func (zpn *ZenPartitionNode) createClusterService(cfg *config.RqLite, ln net.Listener, db cluster.Database, mgr cluster.Manager, credStr *auth.CredentialsStore) (*cluster.Service, error) {
	c := cluster.New(ln, db, mgr, credStr)
	c.EnableHTTPS(cfg.HTTPx509Cert != "" && cfg.HTTPx509Key != "") // Conditions met for an HTTPS API
	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

func (zpn *ZenPartitionNode) createClusterClient(cfg *config.RqLite, clstr *cluster.Service, partition uint32) (*cluster.Client, error) {
	clstrDialer, err := network.NewRqLiteClusterDialer(partition, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert, cfg.NodeVerifyServerName, cfg.NoNodeVerify)
	if err != nil {
		return nil, fmt.Errorf("failed to create RqLite cluster dialer: %s", err.Error())
	}
	clstrClient := cluster.NewClient(clstrDialer, cfg.ClusterConnectTimeout)
	if err := clstrClient.SetLocal(cfg.RaftAdv, clstr); err != nil {
		return nil, fmt.Errorf("failed to set cluster client local parameters: %s", err.Error())
	}
	return clstrClient, nil
}

func (zpn *ZenPartitionNode) createPartitionCluster(ctx context.Context, cfg *config.RqLite, hasPeers bool) error {
	joins := cfg.JoinAddresses()
	if err := zpn.networkCheckJoinAddrs(joins); err != nil {
		return err
	}
	if joins == nil && !hasPeers {
		if cfg.RaftNonVoter {
			return fmt.Errorf("cannot create a new non-voting node without joining it to an existing cluster")
		}

		// Brand new node, told to bootstrap itself. So do it.
		zpn.logger.Info("bootstrapping single new node")
		if err := zpn.store.Bootstrap(store.NewServer(zpn.store.ID(), cfg.RaftAdv, true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	// Prepare definition of being part of a cluster.
	bootDoneFn := func() bool {
		leader, _ := zpn.store.LeaderAddr()
		return leader != ""
	}
	clusterSuf := cluster.VoterSuffrage(!cfg.RaftNonVoter)

	joiner := cluster.NewJoiner(zpn.clusterClient, cfg.JoinAttempts, cfg.JoinInterval)
	joiner.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, cfg.JoinAs))
	if joins != nil && cfg.BootstrapExpect == 0 {
		// Explicit join operation requested, so do it.
		j, err := joiner.Do(ctx, joins, zpn.store.ID(), cfg.RaftAdv, clusterSuf)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		zpn.logger.Info("successfully joined cluster at %s", j)
		return nil
	}

	if joins != nil && cfg.BootstrapExpect > 0 {
		// Bootstrap with explicit join addresses requests.
		bs := cluster.NewBootstrapper(cluster.NewAddressProviderString(joins), zpn.clusterClient)
		bs.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, cfg.JoinAs))
		return bs.Boot(ctx, zpn.store.ID(), cfg.RaftAdv, clusterSuf, bootDoneFn, cfg.BootstrapExpectTimeout)
	}

	return nil
}

func (zpn *ZenPartitionNode) networkCheckJoinAddrs(joinAddrs []string) error {
	if len(joinAddrs) > 0 {
		zpn.logger.Info("checking that supplied join addresses don't serve HTTP(S)")
		if addr, ok := httpd.AnyServingHTTP(joinAddrs); ok {
			return fmt.Errorf("join address %s appears to be serving HTTP when it should be Raft", addr)
		}
	}
	return nil
}
