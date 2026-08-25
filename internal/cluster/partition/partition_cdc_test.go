package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/network"
	zenproto "github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/server/servertest"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
	"github.com/pbinitiative/zenbpm/internal/cluster/types"
	"github.com/pbinitiative/zenbpm/internal/config"
	cdcjson "github.com/rqlite/rqlite/v10/cdc/json"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestZenPartitionNodeCDC(t *testing.T) {
	t.Run("uses the backward-compatible default service ID", func(t *testing.T) {
		serviceID := partitionCDCServiceID("", "", 1)

		require.Equal(t, "zenbpm-partition-1", serviceID)
	})

	t.Run("preserves a legacy rqlite service ID", func(t *testing.T) {
		serviceID := partitionCDCServiceID("legacy-source", "", 2)

		require.Equal(t, "legacy-source-partition-2", serviceID)
	})

	t.Run("uses the configured fallback service ID", func(t *testing.T) {
		serviceID := partitionCDCServiceID("", "configured-source", 3)

		require.Equal(t, "configured-source-partition-3", serviceID)
	})

	t.Run("applies the configured fallback service ID to a direct URL output", func(t *testing.T) {
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer receiver.Close()

		partition, server := prepareCDCPartitionTestSetup(t, config.CDC{
			Enabled:   true,
			Output:    receiver.URL,
			ServiceID: "configured-source",
		})
		defer func() {
			require.NoError(t, partition.Stop())
			require.NoError(t, server.Close())
		}()

		require.NotNil(t, partition.cdcService)
		stats, err := partition.cdcService.Stats()
		require.NoError(t, err)
		require.Equal(t, "configured-source-partition-1", stats["service_id"])
	})

	t.Run("gives the advanced output service ID priority over the configured fallback", func(t *testing.T) {
		received := make(chan cdcjson.CDCMessagesEnvelope, 1)
		receiverErrors := make(chan error, 1)
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var envelope cdcjson.CDCMessagesEnvelope
			if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
				select {
				case receiverErrors <- err:
				default:
				}
				http.Error(w, "invalid CDC payload", http.StatusBadRequest)
				return
			}
			select {
			case received <- envelope:
			default:
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer receiver.Close()

		cdcOutputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		cdcOutput := fmt.Sprintf(
			`{"endpoint":%q,"service_id":"advanced-source","table_filter":"^cdc_test$","max_batch_size":1}`,
			receiver.URL,
		)
		require.NoError(t, os.WriteFile(cdcOutputPath, []byte(cdcOutput), 0o600))

		partition, server := prepareCDCPartitionTestSetup(t, config.CDC{
			Enabled:   true,
			Output:    cdcOutputPath,
			ServiceID: "configured-source",
		})
		defer func() {
			require.NoError(t, partition.Stop())
			require.NoError(t, server.Close())
		}()

		require.NotNil(t, partition.cdcService)
		_, err := partition.DB.ExecContext(context.Background(), `
			CREATE TABLE cdc_test (
				id INTEGER NOT NULL PRIMARY KEY,
				name TEXT NOT NULL
			)
		`)
		require.NoError(t, err)
		_, err = partition.DB.ExecContext(context.Background(), `INSERT INTO cdc_test(id, name) VALUES(1, 'first')`)
		require.NoError(t, err)

		select {
		case err := <-receiverErrors:
			t.Fatalf("CDC receiver failed: %v", err)
		case envelope := <-received:
			require.Equal(t, "advanced-source-partition-1", envelope.ServiceID)
			require.NotEmpty(t, envelope.NodeID)
			require.Len(t, envelope.Payload, 1)
			require.Len(t, envelope.Payload[0].Events, 1)
			event := envelope.Payload[0].Events[0]
			require.Equal(t, "INSERT", event.Op)
			require.Equal(t, "cdc_test", event.Table)
			require.EqualValues(t, 1, event.NewRowID)
			require.Equal(t, "first", event.After["name"])
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for CDC event")
		}
	})
}

func prepareCDCPartitionTestSetup(t *testing.T, cdcConfig config.CDC) (*ZenPartitionNode, *servertest.TestServer) {
	t.Helper()
	ctx := context.Background()
	mux, muxListener, err := network.NewNodeMux("")
	require.NoError(t, err)

	rqLiteConfig := GetRqLiteDefaultConfig(
		"test-rq-lite",
		muxListener.Addr().String(),
		t.TempDir(),
		[]string{muxListener.Addr().String()},
	)
	persistenceConfig := config.Persistence{
		RqLite:           &rqLiteConfig,
		ProcDefCacheTTL:  types.TTL(24 * time.Hour),
		ProcDefCacheSize: 200,
	}

	server := servertest.NewTestServer()
	server.FindActiveMessageHandler = func(_ *zenproto.FindActiveMessageRequest) (*zenproto.FindActiveMessageResponse, error) {
		err := errors.New("message subscription was not found")
		return &zenproto.FindActiveMessageResponse{
			Error: &zenproto.ErrorResult{Message: new(err.Error())},
		}, status.Error(codes.NotFound, err.Error())
	}

	partitionState := state.Partition{Id: 1, LeaderId: "test-rq-lite"}
	tStore := &testStore{
		id:   "test-rq-lite",
		addr: server.Addr(),
		clusterState: state.Cluster{
			Config:     state.ClusterConfig{DesiredPartitions: 1},
			Partitions: map[uint32]state.Partition{1: partitionState},
			Nodes:      map[string]state.Node{},
		},
		leader: true,
	}
	clientManager := client.NewClientManager(tStore)

	partition, err := startZenPartitionNodeWithCDCConfig(
		ctx,
		mux,
		persistenceConfig,
		cdcConfig,
		clientManager,
		1,
		PartitionChangesCallbacks{},
		func() state.Cluster {
			return state.Cluster{
				Config:     state.ClusterConfig{},
				Partitions: map[uint32]state.Partition{1: {Id: 1, LeaderId: "node-1"}},
				Nodes: map[string]state.Node{
					"node-1": {
						Id:         "node-1",
						Addr:       "localhost:",
						Partitions: map[uint32]state.NodePartition{},
					},
				},
			}
		},
		testDBOptions(),
	)
	require.NoError(t, err)
	_, err = partition.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	require.NoError(t, partition.DB.RunMigrations(ctx))
	return partition, server
}
