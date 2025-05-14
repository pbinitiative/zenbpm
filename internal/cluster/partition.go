package cluster

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/rqlite/rqlite-disco-clients/consul"
	"github.com/rqlite/rqlite-disco-clients/dns"
	"github.com/rqlite/rqlite-disco-clients/dnssrv"
	etcd "github.com/rqlite/rqlite-disco-clients/etcd"
	"github.com/rqlite/rqlite/v8/auth"
	"github.com/rqlite/rqlite/v8/auto/backup"
	"github.com/rqlite/rqlite/v8/auto/restore"
	"github.com/rqlite/rqlite/v8/aws"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/command/proto"
	"github.com/rqlite/rqlite/v8/disco"
	httpd "github.com/rqlite/rqlite/v8/http"
	"github.com/rqlite/rqlite/v8/store"
	"github.com/rqlite/rqlite/v8/tcp"
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
}

func (zpn *ZenPartitionNode) RegisterStatus(key string, stat httpd.StatusReporter) error {
	zpn.statusMu.Lock()
	defer zpn.statusMu.Unlock()

	if _, ok := zpn.statuses[key]; ok {
		return fmt.Errorf("status already registered with key %s", key)
	}
	zpn.statuses[key] = stat

	return nil
}

func (zpn *ZenPartitionNode) IsLeader(ctx context.Context) bool {
	return zpn.store.IsLeader()
}

// Execute an SQL statement on rqlite partition node
func (zpn *ZenPartitionNode) Execute(ctx context.Context, req *proto.ExecuteRequest) ([]*proto.ExecuteQueryResponse, error) {
	return zpn.store.Execute(req)
}

// Run an SQL query on rqlite partition node
func (zpn *ZenPartitionNode) Query(ctx context.Context, req *proto.QueryRequest) ([]*proto.QueryRows, error) {
	return zpn.store.Query(req)
}

func StartZenPartitionNode(ctx context.Context, mux *tcp.Mux, cfg *config.RqLite, partition uint32) (*ZenPartitionNode, error) {
	// struct that will hold our partition cluster
	zpn := ZenPartitionNode{
		config:      cfg,
		statuses:    map[string]httpd.StatusReporter{},
		partitionId: partition,
		logger:      hclog.Default().Named(fmt.Sprintf("zen-partition-node-%d", partition)),
	}

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
	zpn.logger.Info(fmt.Sprintf("cluster TCP mux Listener registered with byte header %d", cluster.MuxClusterHeader))

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

	// Register remaining status providers.
	zpn.RegisterStatus("cluster", clstrServ)
	zpn.RegisterStatus("network", tcp.NetworkReporter{})

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
		return nil, fmt.Errorf("failed to start auto-backups: %w", err)
	}
	if backupSrv != nil {
		zpn.RegisterStatus("auto_backups", backupSrv)
	}

	// TODO: remove once engine and persistence start when they should
	str.WaitForLeader(10 * time.Second)
	if str.IsLeader() {
		engine := bpmn.NewEngine(bpmn.EngineWithStorage(zpn.rqliteDB))
		// TODO rework handlers
		emptyHandler := func(job bpmn.ActivatedJob) {
		}
		engine.NewTaskHandler().Type("foo").Handler(emptyHandler)
	}
	return &zpn, nil
}

func (zpn *ZenPartitionNode) WaitForLeader(timeout time.Duration) (string, error) {
	return zpn.store.WaitForLeader(timeout)
}

func (zpn *ZenPartitionNode) Stats() (map[string]interface{}, error) {
	return zpn.clusterClient.Stats()
}

func (zpn *ZenPartitionNode) Stop() error {
	zpn.clusterService.Close()
	if zpn.config.RaftClusterRemoveOnShutdown {
		remover := cluster.NewRemover(zpn.clusterClient, 5*time.Second, zpn.store)
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
	// TODO: register observers for leader change and node changes and propagate them into zen store
	// str.RegisterObserver(...)

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

func (zpn *ZenPartitionNode) createDiscoService(cfg *config.RqLite, str *store.Store) (*disco.Service, error) {
	var c disco.Client
	var err error

	rc := cfg.DiscoConfigReader()
	defer func() {
		if rc != nil {
			rc.Close()
		}
	}()
	if cfg.DiscoMode == config.DiscoModeConsulKV {
		var consulCfg *consul.Config
		consulCfg, err = consul.NewConfigFromReader(rc)
		if err != nil {
			return nil, fmt.Errorf("create Consul config: %s", err.Error())
		}

		c, err = consul.New(cfg.DiscoKey, consulCfg)
		if err != nil {
			return nil, fmt.Errorf("create Consul client: %s", err.Error())
		}
	} else if cfg.DiscoMode == config.DiscoModeEtcdKV {
		var etcdCfg *etcd.Config
		etcdCfg, err = etcd.NewConfigFromReader(rc)
		if err != nil {
			return nil, fmt.Errorf("create etcd config: %s", err.Error())
		}

		c, err = etcd.New(cfg.DiscoKey, etcdCfg)
		if err != nil {
			return nil, fmt.Errorf("create etcd client: %s", err.Error())
		}
	} else {
		return nil, fmt.Errorf("invalid disco service: %s", cfg.DiscoMode)
	}
	return disco.NewService(c, str, disco.VoterSuffrage(!cfg.RaftNonVoter)), nil
}

func (zpn *ZenPartitionNode) createCredentialStore(cfg *config.RqLite) (*auth.CredentialsStore, error) {
	if cfg.AuthFile == "" {
		return nil, nil
	}
	return auth.NewCredentialsStoreFromFile(cfg.AuthFile)
}

func (zpn *ZenPartitionNode) createClusterService(cfg *config.RqLite, ln net.Listener, db cluster.Database, mgr cluster.Manager, credStr *auth.CredentialsStore) (*cluster.Service, error) {
	c := cluster.New(ln, db, mgr, credStr)
	// c.SetAPIAddr(cfg.HTTPAdv)
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
	if joins == nil && cfg.DiscoMode == "" && !hasPeers {
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

	if cfg.DiscoMode == "" {
		// No more clustering techniques to try. Node will just sit, probably using
		// existing Raft state.
		return nil
	}

	// DNS-based discovery requested. It's OK to proceed with this even if this node
	// is already part of a cluster. Re-joining and re-notifying other nodes will be
	// ignored when the node is already part of the cluster.
	zpn.logger.Info(fmt.Sprintf("discovery mode: %s", cfg.DiscoMode))
	switch cfg.DiscoMode {
	case config.DiscoModeDNS, config.DiscoModeDNSSRV:
		rc := cfg.DiscoConfigReader()
		defer func() {
			if rc != nil {
				rc.Close()
			}
		}()

		var provider interface {
			cluster.AddressProvider
			httpd.StatusReporter
		}
		if cfg.DiscoMode == config.DiscoModeDNS {
			dnsCfg, err := dns.NewConfigFromReader(rc)
			if err != nil {
				return fmt.Errorf("error reading DNS configuration: %s", err.Error())
			}
			provider = dns.NewWithPort(dnsCfg, cfg.RaftPort())

		} else {
			dnssrvCfg, err := dnssrv.NewConfigFromReader(rc)
			if err != nil {
				return fmt.Errorf("error reading DNS configuration: %s", err.Error())
			}
			provider = dnssrv.New(dnssrvCfg)
		}

		bs := cluster.NewBootstrapper(provider, zpn.clusterClient)
		bs.SetCredentials(cluster.CredentialsFor(zpn.credentialStore, cfg.JoinAs))
		zpn.RegisterStatus("disco", provider)
		return bs.Boot(ctx, zpn.store.ID(), cfg.RaftAdv, clusterSuf, bootDoneFn, cfg.BootstrapExpectTimeout)

	case config.DiscoModeEtcdKV, config.DiscoModeConsulKV:
		discoService, err := zpn.createDiscoService(cfg, zpn.store)
		if err != nil {
			return fmt.Errorf("failed to start discovery service: %s", err.Error())
		}
		// Safe to start reporting before doing registration. If the node hasn't bootstrapped
		// yet, or isn't leader, reporting will just be a no-op until something changes.
		go discoService.StartReporting(cfg.NodeID, cfg.HTTPURL(), cfg.RaftAdv)
		zpn.RegisterStatus("disco", discoService)

		if hasPeers {
			zpn.logger.Info("preexisting node configuration detected, not registering with discovery service")
			return nil
		}
		zpn.logger.Info("no preexisting nodes, registering with discovery service")

		leader, addr, err := discoService.Register(zpn.store.ID(), cfg.HTTPURL(), cfg.RaftAdv)
		if err != nil {
			return fmt.Errorf("failed to register with discovery service: %s", err.Error())
		}
		if leader {
			zpn.logger.Info("node registered as leader using discovery service")
			if err := zpn.store.Bootstrap(store.NewServer(zpn.store.ID(), zpn.store.Addr(), true)); err != nil {
				return fmt.Errorf("failed to bootstrap single new node: %s", err.Error())
			}
		} else {
			for {
				zpn.logger.Info("discovery service returned %s as join address", addr)
				if j, err := joiner.Do(ctx, []string{addr}, zpn.store.ID(), cfg.RaftAdv, clusterSuf); err != nil {
					zpn.logger.Info("failed to join cluster at %s: %s", addr, err.Error())

					time.Sleep(time.Second)
					_, addr, err = discoService.Register(zpn.store.ID(), cfg.HTTPURL(), cfg.RaftAdv)
					if err != nil {
						zpn.logger.Info(fmt.Sprintf("failed to get updated leader: %s", err.Error()))
					}
					continue
				} else {
					zpn.logger.Info(fmt.Sprintf("successfully joined cluster at %s", j))
					break
				}
			}
		}

	default:
		return fmt.Errorf("invalid disco mode %s", cfg.DiscoMode)
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
