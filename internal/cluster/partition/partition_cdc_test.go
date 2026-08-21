package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	cdcjson "github.com/rqlite/rqlite/v10/cdc/json"
	"github.com/stretchr/testify/require"
)

func TestZenPartitionNodeCDC(t *testing.T) {
	t.Run("uses the backward-compatible default service ID", func(t *testing.T) {
		serviceID := partitionCDCServiceID("", 1)

		require.Equal(t, "zenbpm-partition-1", serviceID)
	})

	t.Run("preserves a legacy rqlite service ID", func(t *testing.T) {
		serviceID := partitionCDCServiceID("legacy-source", 2)

		require.Equal(t, "legacy-source-partition-2", serviceID)
	})

	t.Run("publishes the configured service ID with the partition suffix", func(t *testing.T) {
		received := make(chan cdcjson.CDCMessagesEnvelope, 1)
		receiverErrors := make(chan error, 1)
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()

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

		cdcConfigPath := filepath.Join(t.TempDir(), "cdc.json")
		cdcConfig := fmt.Sprintf(
			`{"endpoint":%q,"service_id":"legacy-rqlite-source","table_filter":"^cdc_test$","max_batch_size":1}`,
			receiver.URL,
		)
		require.NoError(t, os.WriteFile(cdcConfigPath, []byte(cdcConfig), 0o600))

		partition, _, _, _, server := prepareTestSetup(t, false, func(cfg *config.RqLite) {
			cfg.CDCConfig = cdcConfigPath
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
			require.Equal(t, "legacy-rqlite-source-partition-1", envelope.ServiceID)
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
