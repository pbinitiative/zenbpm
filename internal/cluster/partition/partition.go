package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/script"

	"golang.org/x/sync/errgroup"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/safego"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	zproto "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/rqlite/rqlite/v10/auth"
	"github.com/rqlite/rqlite/v10/auto/backup"
	"github.com/rqlite/rqlite/v10/auto/restore"
	"github.com/rqlite/rqlite/v10/cdc"
	"github.com/rqlite/rqlite/v10/cluster"
	"github.com/rqlite/rqlite/v10/command"
	"github.com/rqlite/rqlite/v10/command/proto"
	httpd "github.com/rqlite/rqlite/v10/http"
	"github.com/rqlite/rqlite/v10/store"
	"github.com/rqlite/rqlite/v10/tcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	observerChanLen = 100

	// raftLogFileName / snapshotsDirName mirror the (unexported) names rqlite
	// uses inside the partition data directory. They are only read to derive
	// storage metrics; a rename upstream degrades the metric to 0/-1 rather
	// than breaking the partition.
	raftLogFileName          = "raft.db"
	snapshotsDirName         = "wsnapshots"
	snapshotMetadataFileName = "meta.json"
)

type ZenPartitionNode struct {
	PartitionId     uint32
	config          *config.RqLite
	store           *store.Store
	storeOpen       bool
	DB              *DB
	credentialStore *auth.CredentialsStore
	clusterClient   *cluster.Client
	clusterDialer   *network.ClosableDialer
	clusterService  *cluster.Service
	cdcService      *cdc.Service
	statusMu        sync.Mutex
	statuses        map[string]httpd.StatusReporter
	metrics         partitionMetrics
	cdcMetricsMu    sync.Mutex
	cdcRetriesSeen  uint64
	logger          hclog.Logger

	FeelRuntime script.FeelRuntime
	JsRuntime   script.JsRuntime

	Engine *bpmn.Engine

	observer      *raft.Observer
	observerChan  chan raft.Observation
	observerClose chan struct{}
	observerDone  chan struct{}

	stopOnce sync.Once
	stopErr  error

	// Snapshot observation state is only touched by updatePartitionMetrics,
	// which runs on the single partition observer goroutine.
	snapshotBaselineInitialized bool
	lastSnapshotName            string
	lastSnapshotObservedAt      time.Time

	stateChangeCallbacks PartitionChangesCallbacks
}

func (zpn *ZenPartitionNode) createMetrics() {
	var err error
	zpn.metrics.jobsWaiting, err = otel.Meter(partitionMeter).Int64Gauge("jobs_waiting", metric.WithDescription("Number of jobs waiting to be completed"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for jobsWaiting", "err", err)
	}
	zpn.metrics.processInstancesActive, err = otel.Meter(partitionMeter).Int64Gauge("process_instances_active", metric.WithDescription("Number of process instances in active state"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for jobsWaiting", "err", err)
	}
	zpn.metrics.hasLeader, err = otel.Meter(partitionMeter).Int64Gauge("partition_raft_has_leader", metric.WithDescription("1 when the partition raft group has an elected leader (local raft view; see partition_has_leader for the replicated cluster-state view), 0 otherwise"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for hasLeader", "err", err)
	}
	zpn.metrics.isLeader, err = otel.Meter(partitionMeter).Int64Gauge("partition_node_is_leader", metric.WithDescription("1 when this node is the partition raft leader, 0 otherwise"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for isLeader", "err", err)
	}
	zpn.metrics.dbSize, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_db_size", metric.WithUnit("By"), metric.WithDescription("Size of the partition SQLite database files on disk, bytes"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for dbSize", "err", err)
	}
	zpn.metrics.leaderChanges, err = otel.Meter(partitionMeter).Int64Counter("partition_leader_changes", metric.WithDescription("Number of partition raft leader changes observed by this node"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for leaderChanges", "err", err)
	}
	zpn.metrics.raftLogSize, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_raft_log_size", metric.WithUnit("By"), metric.WithDescription("Size of the partition raft log (bbolt) files on disk, bytes"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for raftLogSize", "err", err)
	}
	zpn.metrics.snapshotAge, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_snapshot_age", metric.WithUnit("s"), metric.WithDescription("Seconds since the newest partition raft snapshot was written; -1 when no snapshot exists yet"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for snapshotAge", "err", err)
	}
	zpn.metrics.snapshotObservationAge, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_snapshot_observation_age", metric.WithUnit("s"), metric.WithDescription("Seconds since this process observed a new completed partition raft snapshot; -1 until one is observed after startup"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for snapshotObservationAge", "err", err)
	}
	zpn.metrics.cdcQueueLength, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_cdc_queue_length", metric.WithDescription("Number of entries waiting in the persistent CDC FIFO on this partition replica"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for cdcQueueLength", "err", err)
	}
	zpn.metrics.cdcHighWatermark, err = otel.Meter(partitionMeter).Int64Gauge("rqlite_cdc_high_watermark", metric.WithDescription("Highest Raft index known to have been delivered by the CDC cluster"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for cdcHighWatermark", "err", err)
	}
	zpn.metrics.cdcEndpointRetries, err = otel.Meter(partitionMeter).Int64Counter("rqlite_cdc_endpoint_retries", metric.WithDescription("Number of retries performed while delivering CDC batches to the configured endpoint"))
	if err != nil {
		zpn.logger.Error("Failed to register meter for cdcEndpointRetries", "err", err)
	}
}

type PartitionChangesCallbacks struct {
	AddNewNode   func(raft.Server) error
	ShutdownNode func(raft.ServerID) error
	LeaderChange func(raft.ServerID) error
	RemoveNode   func(id string) error
	ResumeNode   func(id string) error
}

type partitionMetrics struct {
	jobsWaiting            metric.Int64Gauge
	processInstancesActive metric.Int64Gauge
	// hasLeader reports whether the partition raft group currently has a leader (0/1)
	hasLeader metric.Int64Gauge
	// isLeader reports whether this node is the partition raft leader (0/1)
	isLeader metric.Int64Gauge
	// dbSize reports the size of the partition SQLite database files on disk, in bytes
	dbSize metric.Int64Gauge
	// leaderChanges counts partition raft leader changes observed by this node
	leaderChanges metric.Int64Counter
	// raftLogSize reports the size of the partition raft log files on disk, in bytes
	raftLogSize metric.Int64Gauge
	// snapshotAge reports the age of the newest partition raft snapshot, in seconds
	snapshotAge metric.Int64Gauge
	// snapshotObservationAge reports the time since this process observed a new completed snapshot
	snapshotObservationAge metric.Int64Gauge
	// cdcQueueLength reports the number of entries in this replica's persistent CDC FIFO
	cdcQueueLength metric.Int64Gauge
	// cdcHighWatermark reports the highest Raft index delivered by the CDC cluster
	cdcHighWatermark metric.Int64Gauge
	// cdcEndpointRetries counts endpoint retries performed by this partition replica
	cdcEndpointRetries metric.Int64Counter
}

type cdcMetricsSource interface {
	Stats() (map[string]any, error)
	HighWatermark() uint64
	NumEndpointRetries() uint64
}

const (
	partitionMeter string = "partition"
)

func StartZenPartitionNode(ctx context.Context, mux *tcp.Mux, persistenceConfig config.Persistence, cdcConfig config.CDC, client *client.ClientManager, partition uint32, callbacks PartitionChangesCallbacks, zenState func() state.Cluster) (*ZenPartitionNode, error) {
	return startZenPartitionNodeWithCDCConfig(ctx, mux, persistenceConfig, cdcConfig, client, partition, callbacks, zenState, defaultDBOptions())
}

func startZenPartitionNode(ctx context.Context, mux *tcp.Mux, persistenceConfig config.Persistence, client *client.ClientManager, partition uint32, callbacks PartitionChangesCallbacks, zenState func() state.Cluster, dbOpts dbOptions) (_ *ZenPartitionNode, err error) {
	return startZenPartitionNodeWithCDCConfig(ctx, mux, persistenceConfig, config.CDC{}, client, partition, callbacks, zenState, dbOpts)
}

func startZenPartitionNodeWithCDCConfig(ctx context.Context, mux *tcp.Mux, persistenceConfig config.Persistence, cdcConfig config.CDC, client *client.ClientManager, partition uint32, callbacks PartitionChangesCallbacks, zenState func() state.Cluster, dbOpts dbOptions) (_ *ZenPartitionNode, err error) {
	cfg := persistenceConfig.RqLite
	zpn := ZenPartitionNode{
		config:               cfg,
		statuses:             map[string]httpd.StatusReporter{},
		PartitionId:          partition,
		logger:               hclog.Default().Named(fmt.Sprintf("zen-partition-node-%d", partition)),
		stateChangeCallbacks: callbacks,
	}
	defer func() {
		if err == nil {
			return
		}
		if cleanupErr := zpn.Stop(); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to clean up partition after startup error: %w", cleanupErr))
		}
	}()
	zpn.logger.Info(fmt.Sprintf("Starting partition %d node", partition))

	zpn.createMetrics()

	raftLn := network.NewRqLiteRaftListener(partition, mux)
	raftDialer, err := network.NewRqLiteRaftDialer(partition, cfg.NodeX509Cert, cfg.NodeX509Key, cfg.NodeX509CACert,
		cfg.NodeVerifyServerName, cfg.NoNodeVerify)
	if err != nil {
		return nil, fmt.Errorf("failed to create RqLite Raft dialer: %w", err)
	}
	raftTn := tcp.NewLayer(raftLn, raftDialer)

	str, err := zpn.createStore(cfg, raftTn, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	zpn.store = str

	zpn.DB, err = newDB(
		zpn.store,
		zpn.PartitionId,
		hclog.Default().Named(fmt.Sprintf("zen-partition-sql-%d", partition)),
		persistenceConfig,
		client,
		zenState,
		dbOpts,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create rqLiteDB for partition %d: %w", partition, err)
	}

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

	// We want to start the HTTP server as soon as possible, so the node is responsive and external
	// systems can see that it's running. We still have to open the Store though, so the node won't
	// be able to do much until that happens however.
	clstrClient, err := zpn.createClusterClient(cfg, clstrServ, partition)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster client: %w", err)
	}
	zpn.clusterClient = clstrClient

	if cdcConfig.Output != "" {
		cdcService, err := zpn.createCDCService(cfg, cdcConfig, str, clstrServ, clstrClient, partition)
		if err != nil {
			return nil, fmt.Errorf("failed to create CDC service: %w", err)
		}
		zpn.cdcService = cdcService
	}

	// Now, open store. How long this takes does depend on how much data is being stored by rqlite.
	if err := str.Open(); err != nil {
		return nil, fmt.Errorf("failed to open store: %w", err)
	}
	zpn.storeOpen = true

	zpn.observerChan = make(chan raft.Observation, observerChanLen)
	zpn.observer = raft.NewObserver(zpn.observerChan, true, func(o *raft.Observation) bool {
		_, isLeaderChange := o.Data.(raft.LeaderObservation)
		_, isFailedHeartBeat := o.Data.(raft.FailedHeartbeatObservation)
		_, isResumedHeartBeat := o.Data.(raft.ResumedHeartbeatObservation)
		_, isPeerChange := o.Data.(raft.PeerObservation)
		return isLeaderChange || isFailedHeartBeat || isResumedHeartBeat || isPeerChange
	})
	str.RegisterObserver(zpn.observer)
	zpn.observerClose, zpn.observerDone = zpn.observe()

	if err := zpn.registerStatus("cluster", clstrServ); err != nil {
		return nil, fmt.Errorf("failed to register cluster status reporter: %w", err)
	}
	if err := zpn.registerStatus("network", tcp.NetworkReporter{}); err != nil {
		return nil, fmt.Errorf("failed to register network status reporter: %w", err)
	}
	if zpn.cdcService != nil {
		if err := zpn.registerStatus("cdc", zpn.cdcService); err != nil {
			return nil, fmt.Errorf("failed to register CDC status provider: %w", err)
		}
	}

	nodes, err := str.Nodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes %w", err)
	}
	if err := zpn.createPartitionCluster(ctx, cfg, len(nodes) > 0); err != nil {
		return nil, fmt.Errorf("clustering failure: %w", err)
	}

	backupSrv, err := zpn.startAutoBackups(ctx, cfg, str)
	if err != nil {
		return nil, fmt.Errorf("failed to start auto-backups: %w", err)
	}
	if backupSrv != nil {
		if err := zpn.registerStatus("auto_backups", backupSrv); err != nil {
			return nil, fmt.Errorf("failed to register auto_backups status reporter: %w", err)
		}
	}
	return &zpn, nil
}

func (zpn *ZenPartitionNode) IsLeader(ctx context.Context) bool {
	return zpn.store.IsLeader()
}

// Health reports whether this node currently sees a working partition Raft
// from its own perspective: either it is the Leader, or there is a known
// leader (heartbeats arriving). Anything else means no quorum from this
// node's POV.
func (zpn *ZenPartitionNode) Health() (ok bool, reason string) {
	if zpn.store.IsLeader() {
		return true, ""
	}
	if zpn.store.HasLeader() {
		return true, ""
	}
	return false, fmt.Sprintf("partition raft state %v, no leader known", zpn.store.State())
}

func (zpn *ZenPartitionNode) Role() zproto.Role {
	if zpn.store.IsLeader() {
		return zproto.Role_ROLE_TYPE_LEADER
	}
	return zproto.Role_ROLE_TYPE_FOLLOWER
}

func (zpn *ZenPartitionNode) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	res, _, err := zpn.store.Execute(ctx, req)
	return res, err
}

func (zpn *ZenPartitionNode) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	rows, _, _, err := zpn.store.Query(ctx, req)
	return rows, err
}

func (zpn *ZenPartitionNode) WaitForLeader(timeout time.Duration) (string, error) {
	return zpn.store.WaitForLeader(timeout)
}

func (zpn *ZenPartitionNode) Stats() (map[string]interface{}, error) {
	return zpn.clusterClient.Stats()
}

// Stop shuts the partition node down and releases its resources. It is safe to
// call from multiple goroutines: the node is reachable both from Controller.Stop
// and from the partition leaving handler, and the shutdown sequence closes
// channels that must not be closed twice.
func (zpn *ZenPartitionNode) Stop() error {
	if zpn == nil {
		return nil
	}
	zpn.stopOnce.Do(func() {
		zpn.stopErr = zpn.stop()
	})
	return zpn.stopErr
}

func (zpn *ZenPartitionNode) stop() error {
	var stopErr error

	if zpn.Engine != nil {
		zpn.Engine.Stop()
	}
	if zpn.DB != nil {
		zpn.DB.Close()
	}
	if zpn.FeelRuntime != nil {
		zpn.FeelRuntime.Stop()
	}
	if zpn.JsRuntime != nil {
		zpn.JsRuntime.Stop()
	}
	if zpn.observer != nil && zpn.storeOpen {
		zpn.store.DeregisterObserver(zpn.observer)
		zpn.observer = nil
	}
	if zpn.observerClose != nil {
		close(zpn.observerClose)
		<-zpn.observerDone
		zpn.observerClose = nil
		zpn.observerDone = nil
	}

	if zpn.storeOpen {
		standalone := zpn.store.IsStandalone()
		if zpn.config.RaftClusterRemoveOnShutdown && !standalone {
			remover := cluster.NewRemover(zpn.clusterClient, 1*time.Second, zpn.store)
			remover.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, zpn.config.JoinAs))
			zpn.logger.Info("initiating removal of this node from cluster before shutdown")
			removeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := remover.Do(removeCtx, zpn.config.NodeID, true)
			cancel()
			if err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("failed to remove this node from cluster before shutdown: %w", err))
			} else {
				zpn.logger.Info("removed this node successfully from cluster before shutdown")
			}
		} else if zpn.config.RaftClusterRemoveOnShutdown {
			zpn.logger.Info("skipping removal of the only voting node during shutdown")
		}

		if zpn.config.RaftStepdownOnShutdown && !standalone && zpn.store.IsLeader() {
			zpn.logger.Info("stepping down as Leader before shutdown")
			if err := zpn.store.Stepdown(true, ""); err != nil {
				stopErr = errors.Join(stopErr, fmt.Errorf("failed to step down partition leader: %w", err))
			}
		}
	}

	if zpn.clusterDialer != nil {
		if err := zpn.clusterDialer.Close(); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("failed to close RQLite cluster connections: %w", err))
		}
	}
	if zpn.clusterService != nil {
		if err := zpn.clusterService.Close(); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("failed to close RQLite cluster service: %w", err))
		}
	}
	if zpn.storeOpen {
		if err := zpn.store.Close(true); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("failed to close RQLite store: %w", err))
		} else {
			zpn.storeOpen = false
		}
	}
	if zpn.cdcService != nil {
		// Capture retries and delivery progress since the observer's final
		// five-second collection before stopping the service.
		zpn.updateCDCMetrics(context.Background())
		// Keep CDC running until Store.Close completes. RqLite performs a
		// snapshot-on-close and waits for CDC to flush its persistent queue
		// before taking that snapshot.
		zpn.cdcService.Stop()
		zpn.cdcService = nil
	}

	zpn.logger.Info("rqlite server stopped")
	return stopErr
}

func (zpn *ZenPartitionNode) createCDCService(
	cfg *config.RqLite,
	cdcSettings config.CDC,
	str *store.Store,
	clstrServ *cluster.Service,
	clstrClient *cluster.Client,
	partition uint32,
) (*cdc.Service, error) {
	cdcConfig, err := cdc.NewConfig(cdcSettings.Output)
	if err != nil {
		return nil, fmt.Errorf("failed to load CDC output: %w", err)
	}

	baseServiceID, err := cdcSettings.ResolveServiceID(cdcConfig.ServiceID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve CDC service ID: %w", err)
	}
	cdcConfig.ServiceID = partitionCDCServiceID(baseServiceID, partition)

	cdcCluster := cdc.NewCDCCluster(str, clstrServ, clstrClient)
	cdcService, err := cdc.NewService(cfg.NodeID, cfg.DataPath, cdcCluster, cdcConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize CDC service: %w", err)
	}
	if err := cdcService.Start(); err != nil {
		return nil, fmt.Errorf("failed to start CDC service: %w", err)
	}

	var tableFilter *regexp.Regexp
	if cdcConfig.TableFilter != nil {
		tableFilter = cdcConfig.TableFilter.Regexp
	}
	if err := str.EnableCDC(cdcService.C(), tableFilter, cdcConfig.RowIDsOnly); err != nil {
		cdcService.Stop()
		return nil, fmt.Errorf("failed to enable CDC on store: %w", err)
	}
	return cdcService, nil
}

func partitionCDCServiceID(serviceID string, partition uint32) string {
	return fmt.Sprintf("%s-partition-%d", serviceID, partition)
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

	uCfg, sc, err := backup.NewStorageClient(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse auto-backup file: %s", err.Error())
	}
	provider := store.NewProvider(str, uCfg.Vacuum, !uCfg.NoCompress)
	u := backup.NewUploader(sc, provider, time.Duration(uCfg.Interval))
	u.Start(ctx, str.IsLeader)
	return u, nil
}

func (zpn *ZenPartitionNode) createStore(cfg *config.RqLite, ln *tcp.Layer, partition uint32) (*store.Store, error) {
	dbConf := store.NewDBConfig()
	dbConf.FKConstraints = cfg.FKConstraints

	str := store.New(&store.Config{
		DBConf: dbConf,
		Dir:    cfg.DataPath,
		ID:     cfg.NodeID,
		Logger: hclog.Default().
			Named(fmt.Sprintf("rqlite-partition-store-%d", partition)).
			StandardLogger(&hclog.StandardLoggerOptions{
				ForceLevel: hclog.Default().GetLevel(),
			}),
	}, ln)

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
	ticker := time.NewTicker(5 * time.Second)

	safego.Go("partition-observer", zpn.logger, func() {
		defer close(doneCh)
		defer ticker.Stop()
		var lastObservedLeaderID string
		for {
			select {
			case <-ticker.C:
				zpn.updatePartitionMetrics()
			case o := <-zpn.observerChan:
				switch signal := o.Data.(type) {
				case raft.ResumedHeartbeatObservation:
					if zpn.stateChangeCallbacks.ResumeNode == nil {
						break
					}
					if err := zpn.stateChangeCallbacks.ResumeNode(string(signal.PeerID)); err == nil {
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

					if zpn.stateChangeCallbacks.ShutdownNode != nil && zpn.config.RaftHeartbeatShutdownTimeout > 0 && dur > zpn.config.RaftHeartbeatShutdownTimeout {
						if err = zpn.stateChangeCallbacks.ShutdownNode(signal.PeerID); err == nil {
							zpn.logger.Info(fmt.Sprintf("partition node %s was shutdown in the state", signal.PeerID))
						}
					}

					if zpn.stateChangeCallbacks.RemoveNode != nil && ((isReadOnly && zpn.config.RaftReapReadOnlyNodeTimeout > 0 && dur > zpn.config.RaftReapReadOnlyNodeTimeout) ||
						(!isReadOnly && zpn.config.RaftReapNodeTimeout > 0 && dur > zpn.config.RaftReapNodeTimeout)) {
						pn := "voting node"
						if isReadOnly {
							pn = "non-voting node"
						}
						if err := zpn.stateChangeCallbacks.RemoveNode(id); err != nil {
							zpn.logger.Error(fmt.Sprintf("failed to reap partition node %s %s: %s", pn, id, err.Error()))
						} else {
							zpn.logger.Info(fmt.Sprintf("successfully reaped partition node %s %s", pn, id))
						}
					}
				case raft.LeaderObservation:
					newLeaderID := string(signal.LeaderID)
					if zpn.metrics.leaderChanges != nil && shouldRecordLeaderChange(lastObservedLeaderID, newLeaderID) {
						zpn.metrics.leaderChanges.Add(context.Background(), 1, metric.WithAttributes(
							attribute.Int64("partition", int64(zpn.PartitionId)),
						))
					}
					if newLeaderID != "" {
						lastObservedLeaderID = newLeaderID
					}
					if zpn.stateChangeCallbacks.LeaderChange == nil {
						break
					}
					if err := zpn.stateChangeCallbacks.LeaderChange(signal.LeaderID); err != nil {
						zpn.logger.Error("failed to publish partition leader change", "partition", zpn.PartitionId, "leaderId", signal.LeaderID, "err", err)
					}
				case raft.PeerObservation:
					var err error
					if signal.Removed && zpn.stateChangeCallbacks.ShutdownNode != nil {
						if err = zpn.stateChangeCallbacks.ShutdownNode(signal.Peer.ID); err == nil {
							zpn.logger.Info(fmt.Sprintf("partition node %s was shutdown in the state", signal.Peer.ID))
						}
					} else if !signal.Removed && zpn.stateChangeCallbacks.AddNewNode != nil {
						if err = zpn.stateChangeCallbacks.AddNewNode(signal.Peer); err == nil {
							zpn.logger.Debug(fmt.Sprintf("partition node %s was updated in the state", signal.Peer.ID))
						}
					}
					if err != nil {
						zpn.logger.Error(fmt.Sprintf("failed to update peer observation in partition %d: %s", zpn.PartitionId, err))
					}
				}
			case <-closeCh:
				return
			}
		}
	})
	return closeCh, doneCh
}

func shouldRecordLeaderChange(previousLeaderID, newLeaderID string) bool {
	return previousLeaderID != "" && newLeaderID != "" && previousLeaderID != newLeaderID
}

func (zpn *ZenPartitionNode) updatePartitionMetrics() {
	ctx := context.Background()

	partitionAttr := metric.WithAttributes(attribute.Int64("partition", int64(zpn.PartitionId)))
	zpn.updateCDCMetrics(ctx)
	if zpn.metrics.hasLeader != nil {
		leaderAddr, _ := zpn.store.LeaderAddr()
		hasLeader := int64(0)
		if leaderAddr != "" {
			hasLeader = 1
		}
		zpn.metrics.hasLeader.Record(ctx, hasLeader, partitionAttr)
	}
	if zpn.metrics.isLeader != nil {
		isLeader := int64(0)
		if zpn.store.IsLeader() {
			isLeader = 1
		}
		zpn.metrics.isLeader.Record(ctx, isLeader, partitionAttr)
	}
	if zpn.metrics.dbSize != nil {
		if size, err := zpn.dbSizeBytes(); err != nil {
			zpn.logger.Warn("Failed to compute rqlite db size", "partition", zpn.PartitionId, "err", err)
		} else {
			zpn.metrics.dbSize.Record(ctx, size, partitionAttr)
		}
	}
	zpn.updateRaftStorageMetrics(ctx, partitionAttr)

	g, gCtx := errgroup.WithContext(ctx)

	var waitingJobs int64
	g.Go(func() error {
		return safego.Run("partition-metrics-waiting-jobs", zpn.logger, func() error {
			var err error
			waitingJobs, err = zpn.DB.Queries.CountWaitingJobs(gCtx)
			return err
		})
	})

	var activeInstances int64
	g.Go(func() error {
		return safego.Run("partition-metrics-active-instances", zpn.logger, func() error {
			var err error
			activeInstances, err = zpn.DB.Queries.CountActiveProcessInstances(gCtx)
			return err
		})
	})

	if err := g.Wait(); err != nil {
		zpn.logger.Error("Failed to update metrics for partition", "partition", zpn.PartitionId, "err", err)
		return
	}
	zpn.metrics.jobsWaiting.Record(ctx, waitingJobs, metric.WithAttributes(
		attribute.Int64("partition", int64(zpn.PartitionId)),
	))
	zpn.metrics.processInstancesActive.Record(ctx, activeInstances, metric.WithAttributes(
		attribute.Int64("partition", int64(zpn.PartitionId)),
	))
}

func (zpn *ZenPartitionNode) updateCDCMetrics(ctx context.Context) {
	if zpn.cdcService == nil {
		return
	}
	zpn.recordCDCMetrics(ctx, zpn.cdcService)
}

func (zpn *ZenPartitionNode) recordCDCMetrics(ctx context.Context, source cdcMetricsSource) {
	if source == nil {
		return
	}

	nodeID := ""
	if zpn.config != nil {
		nodeID = zpn.config.NodeID
	}
	attrs := metric.WithAttributes(
		attribute.Int64("partition", int64(zpn.PartitionId)),
		attribute.String("node_id", nodeID),
	)

	stats, err := source.Stats()
	if err != nil {
		if zpn.logger != nil {
			zpn.logger.Warn("Failed to retrieve CDC metrics", "partition", zpn.PartitionId, "nodeId", nodeID, "err", err)
		}
	} else if zpn.metrics.cdcQueueLength != nil {
		if queueLength, ok := cdcQueueLength(stats); ok {
			zpn.metrics.cdcQueueLength.Record(ctx, queueLength, attrs)
		} else if zpn.logger != nil {
			zpn.logger.Warn("CDC status did not contain a valid FIFO length", "partition", zpn.PartitionId, "nodeId", nodeID)
		}
	}

	if zpn.metrics.cdcHighWatermark != nil {
		zpn.metrics.cdcHighWatermark.Record(ctx, clampUint64ToInt64(source.HighWatermark()), attrs)
	}
	if zpn.metrics.cdcEndpointRetries != nil {
		if retryDelta := zpn.cdcRetryDelta(source); retryDelta > 0 {
			zpn.metrics.cdcEndpointRetries.Add(ctx, clampUint64ToInt64(retryDelta), attrs)
		}
	}
}

func (zpn *ZenPartitionNode) cdcRetryDelta(source cdcMetricsSource) uint64 {
	zpn.cdcMetricsMu.Lock()
	defer zpn.cdcMetricsMu.Unlock()

	// Read the source counter while holding the lock so concurrent collectors
	// cannot apply newer and older absolute values out of order.
	current := source.NumEndpointRetries()
	previous := zpn.cdcRetriesSeen
	zpn.cdcRetriesSeen = current
	if current >= previous {
		return current - previous
	}
	// Treat a lower value as a service-counter reset. The new service's current
	// value is the complete delta since that reset.
	return current
}

func cdcQueueLength(stats map[string]any) (int64, bool) {
	fifo, ok := stats["fifo"].(map[string]any)
	if !ok {
		return 0, false
	}

	switch length := fifo["length"].(type) {
	case int:
		if length < 0 {
			return 0, false
		}
		return int64(length), true
	case int64:
		return length, length >= 0
	case uint64:
		return clampUint64ToInt64(length), true
	default:
		return 0, false
	}
}

func clampUint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

// dbSizeBytes returns the combined on-disk size of the partition SQLite
// database files (main db + WAL/SHM) in the rqlite data directory.
func (zpn *ZenPartitionNode) dbSizeBytes() (int64, error) {
	if zpn.config == nil || zpn.config.DataPath == "" {
		return 0, fmt.Errorf("no data path configured")
	}
	matches, err := filepath.Glob(filepath.Join(zpn.config.DataPath, "db.sqlite*"))
	if err != nil {
		return 0, fmt.Errorf("failed to glob sqlite files: %w", err)
	}
	if len(matches) == 0 {
		return 0, fmt.Errorf("no sqlite database files matching %q found in %s", "db.sqlite*", zpn.config.DataPath)
	}
	var total int64
	var files int
	for _, match := range matches {
		info, err := os.Stat(match)
		if err != nil {
			if os.IsNotExist(err) {
				zpn.logger.Debug("sqlite file disappeared before stat during db size metric collection", "file", match)
				continue
			}
			return 0, fmt.Errorf("failed to stat sqlite file %s for db size metric: %w", match, err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		files++
		total += info.Size()
	}
	if files == 0 {
		return 0, fmt.Errorf("no readable sqlite database files matching %q found in %s", "db.sqlite*", zpn.config.DataPath)
	}
	return total, nil
}

// updateRaftStorageMetrics reports the on-disk raft log size, the age of the
// newest raft snapshot and the time since this process observed a new snapshot.
// All three are derived from the files rqlite maintains in the partition data
// directory, which is far cheaper than the full store.Stats() call.
func (zpn *ZenPartitionNode) updateRaftStorageMetrics(ctx context.Context, partitionAttr metric.RecordOption) {
	zpn.recordRaftLogSize(ctx, partitionAttr)
	if zpn.metrics.snapshotAge == nil && zpn.metrics.snapshotObservationAge == nil {
		return
	}
	name, modTime, err := zpn.newestSnapshot()
	if err != nil {
		zpn.logger.Warn("Failed to inspect rqlite snapshots", "partition", zpn.PartitionId, "err", err)
		return
	}
	zpn.observeSnapshotName(name)
	zpn.recordSnapshotAges(ctx, partitionAttr, name, modTime)
}

func (zpn *ZenPartitionNode) recordRaftLogSize(ctx context.Context, partitionAttr metric.RecordOption) {
	if zpn.metrics.raftLogSize == nil {
		return
	}
	size, err := zpn.raftLogSizeBytes()
	if err != nil {
		zpn.logger.Warn("Failed to compute rqlite raft log size", "partition", zpn.PartitionId, "err", err)
		return
	}
	zpn.metrics.raftLogSize.Record(ctx, size, partitionAttr)
}

func (zpn *ZenPartitionNode) recordSnapshotAges(ctx context.Context, partitionAttr metric.RecordOption, name string, modTime time.Time) {
	if zpn.metrics.snapshotAge != nil {
		age := int64(-1)
		if name != "" {
			age = int64(time.Since(modTime).Seconds())
		}
		zpn.metrics.snapshotAge.Record(ctx, age, partitionAttr)
	}
	if zpn.metrics.snapshotObservationAge != nil {
		age := int64(-1)
		if !zpn.lastSnapshotObservedAt.IsZero() {
			age = int64(time.Since(zpn.lastSnapshotObservedAt).Seconds())
		}
		zpn.metrics.snapshotObservationAge.Record(ctx, age, partitionAttr)
	}
}

// observeSnapshotName establishes a startup baseline before recording changes,
// so a snapshot restored from disk is not reported as newly observed.
func (zpn *ZenPartitionNode) observeSnapshotName(name string) {
	if !zpn.snapshotBaselineInitialized {
		zpn.snapshotBaselineInitialized = true
		zpn.lastSnapshotName = name
		return
	}
	if name != "" && name != zpn.lastSnapshotName {
		zpn.lastSnapshotName = name
		zpn.lastSnapshotObservedAt = time.Now()
	}
}

// raftLogSizeBytes returns the on-disk size of the partition raft log (bbolt)
// files in the rqlite data directory.
func (zpn *ZenPartitionNode) raftLogSizeBytes() (int64, error) {
	if zpn.config == nil || zpn.config.DataPath == "" {
		return 0, fmt.Errorf("no data path configured")
	}
	info, err := os.Stat(filepath.Join(zpn.config.DataPath, raftLogFileName))
	if err != nil {
		if os.IsNotExist(err) {
			// the log store is created on open; report 0 until then instead of
			// spamming warnings during startup
			return 0, nil
		}
		return 0, fmt.Errorf("failed to stat raft log file for size metric: %w", err)
	}
	return info.Size(), nil
}

// newestSnapshot returns the name and modification time of the most recently
// written raft snapshot of this partition. Both are zero values when the node
// has not snapshotted yet, which is not an error.
func (zpn *ZenPartitionNode) newestSnapshot() (string, time.Time, error) {
	root, entries, err := zpn.snapshotEntries()
	if err != nil {
		return "", time.Time{}, err
	}
	if root == nil {
		// the snapshot directory does not exist yet
		return "", time.Time{}, nil
	}
	defer func() { _ = root.Close() }()

	var newestName string
	var newestMod time.Time
	for _, entry := range entries {
		modTime, completed, err := completedSnapshotModTime(root, entry)
		if err != nil {
			return "", time.Time{}, err
		}
		if completed && (newestName == "" || modTime.After(newestMod)) {
			newestName = entry.Name()
			newestMod = modTime
		}
	}
	return newestName, newestMod, nil
}

// snapshotEntries lists the partition snapshot directory and opens it as an
// os.Root. Snapshot metadata is then read through that root, so a symlinked
// entry inside the directory cannot redirect the read somewhere else, and the
// directory stays pinned even if it is moved while it is being scanned.
// A nil root reports that the directory does not exist yet, which is not an
// error: rqlite creates it together with the first snapshot.
func (zpn *ZenPartitionNode) snapshotEntries() (*os.Root, []os.DirEntry, error) {
	if zpn.config == nil || zpn.config.DataPath == "" {
		return nil, nil, fmt.Errorf("no data path configured")
	}
	dir := filepath.Join(zpn.config.DataPath, snapshotsDirName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to read snapshot directory %s: %w", dir, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		if os.IsNotExist(err) {
			// reaped between the listing and this call
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to open snapshot directory %s: %w", dir, err)
	}
	return root, entries, nil
}

func completedSnapshotModTime(root *os.Root, entry os.DirEntry) (time.Time, bool, error) {
	if !entry.IsDir() || !isSnapshotID(entry.Name()) {
		return time.Time{}, false, nil
	}
	validMetadata, err := validSnapshotMetadata(root, entry.Name())
	if err != nil {
		return time.Time{}, false, err
	}
	if !validMetadata {
		return time.Time{}, false, nil
	}
	info, err := entry.Info()
	if err != nil {
		if os.IsNotExist(err) {
			// The snapshot was reaped between ReadDir and Info.
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("failed to stat snapshot %s: %w", entry.Name(), err)
	}
	return info.ModTime(), true, nil
}

// validSnapshotMetadata reports whether the snapshot directory holds rqlite
// metadata identifying it as the completed snapshot of that name. The metadata
// is read relative to root, which confines the read to the snapshot directory.
func validSnapshotMetadata(root *os.Root, snapshotID string) (bool, error) {
	path := filepath.Join(snapshotID, snapshotMetadataFileName)
	// Lstat, not Stat: rqlite always writes meta.json as a regular file, so a
	// symlink is treated as an incomplete snapshot instead of being followed.
	info, err := root.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to stat snapshot metadata %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return false, nil
	}

	file, err := root.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to open snapshot metadata %s: %w", path, err)
	}
	defer func() { _ = file.Close() }()

	var metadata raft.SnapshotMeta
	if err := json.NewDecoder(file).Decode(&metadata); err != nil {
		return false, fmt.Errorf("failed to decode snapshot metadata %s: %w", path, err)
	}
	return metadata.ID == snapshotID, nil
}

// isSnapshotID recognizes rqlite's completed snapshot directory names:
// <raft-term>-<raft-index>-<creation-time-unix-milliseconds>.
func isSnapshotID(name string) bool {
	parts := strings.Split(name, "-")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func (zpn *ZenPartitionNode) createCredentialStore(cfg *config.RqLite) (*auth.CredentialsStore, error) {
	if cfg.AuthFile == "" {
		return nil, nil
	}
	return auth.NewCredentialsStoreFromFile(cfg.AuthFile)
}

func (zpn *ZenPartitionNode) createClusterService(cfg *config.RqLite, ln net.Listener, db cluster.Database, mgr cluster.Manager, credStr *auth.CredentialsStore) (*cluster.Service, error) {
	c := cluster.New(ln, db, mgr, credStr)
	c.EnableHTTPS(cfg.HTTPx509Cert != "" && cfg.HTTPx509Key != "")
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
	zpn.clusterDialer = clstrDialer
	clstrClient := cluster.NewClient(clstrDialer, cfg.ClusterConnectTimeout)
	if err := clstrClient.SetLocal(cfg.RaftAdv, clstr); err != nil {
		_ = clstrDialer.Close()
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

		zpn.logger.Info("bootstrapping single new node")
		if err := zpn.store.Bootstrap(store.NewServer(zpn.store.ID(), cfg.RaftAdv, true)); err != nil {
			return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
		}
		return nil
	}

	bootDoneFn := func() bool {
		leader, _ := zpn.store.LeaderAddr()
		return leader != ""
	}
	clusterSuf := command.SuffrageVoterFromBool(!cfg.RaftNonVoter)

	joiner := cluster.NewJoiner(zpn.clusterClient, cfg.JoinAttempts, cfg.JoinInterval)
	joiner.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, cfg.JoinAs))
	if joins != nil && cfg.BootstrapExpect == 0 {
		j, err := joiner.Do(ctx, joins, zpn.store.ID(), cfg.RaftAdv, clusterSuf)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %s", err.Error())
		}
		zpn.logger.Info("successfully joined cluster at %s", j)
		return nil
	}

	if joins != nil && cfg.BootstrapExpect > 0 {
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
