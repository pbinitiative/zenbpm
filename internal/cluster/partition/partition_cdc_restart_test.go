package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	cdcjson "github.com/rqlite/rqlite/v10/cdc/json"
	"github.com/stretchr/testify/require"
)

func TestZenPartitionNodeCDCRestart(t *testing.T) {
	t.Run("preserves filtered row ID delivery after reopening the same data directory", func(t *testing.T) {
		var deliveries cdcRestartDeliveries
		receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var envelope cdcjson.CDCMessagesEnvelope
			err := json.NewDecoder(r.Body).Decode(&envelope)
			deliveries.mu.Lock()
			defer deliveries.mu.Unlock()
			if err != nil {
				deliveries.decodeErr = err
				http.Error(w, "invalid CDC payload", http.StatusBadRequest)
				return
			}
			deliveries.envelopes = append(deliveries.envelopes, envelope)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(receiver.Close)

		outputPath := filepath.Join(t.TempDir(), "cdc-output.json")
		output := fmt.Sprintf(
			`{"endpoint":%q,"table_filter":"^cdc_restart_included$","row_ids_only":true,"max_batch_size":1,"max_batch_delay":%d}`,
			receiver.URL, 5*time.Millisecond,
		)
		require.NoError(t, os.WriteFile(outputPath, []byte(output), 0o600))
		settings := config.CDC{Enabled: true, Output: outputPath, ServiceID: "restart-source"}
		dataPath := t.TempDir()
		firstFixture := prepareCDCPartitionTestFixture(t, dataPath, "")
		first, stopFirst := startCDCPartitionWithinDeadline(t, firstFixture, settings)
		_, err := first.DB.ExecContext(t.Context(), `
			CREATE TABLE cdc_restart_excluded (id INTEGER PRIMARY KEY, name TEXT);
			CREATE TABLE cdc_restart_included (id INTEGER PRIMARY KEY, name TEXT);
		`)
		require.NoError(t, err)
		_, err = first.DB.ExecContext(t.Context(), `
			INSERT INTO cdc_restart_excluded VALUES(10, 'excluded before restart');
			INSERT INTO cdc_restart_included VALUES(1, 'before restart');
		`)
		require.NoError(t, err)
		requireCDCRestartDelivery(t, &deliveries, 1)

		stopFirst()
		require.NoError(t, firstFixture.closeNetwork())
		reopenedFixture := prepareCDCPartitionTestFixture(t, dataPath, firstFixture.address)
		reopened, stopReopened := startCDCPartitionWithinDeadline(t, reopenedFixture, settings)
		var persistedName string
		err = reopened.DB.QueryRowContext(t.Context(), "SELECT name FROM cdc_restart_included WHERE id = 1").Scan(&persistedName)
		require.NoError(t, err)
		require.Equal(t, "before restart", persistedName)

		_, err = reopened.DB.ExecContext(t.Context(), `
			INSERT INTO cdc_restart_excluded VALUES(11, 'excluded after restart');
			INSERT INTO cdc_restart_included VALUES(2, 'after restart');
		`)
		require.NoError(t, err)
		requireCDCRestartDelivery(t, &deliveries, 2)
		stopReopened()
		requireCDCRestartDelivery(t, &deliveries, 2)
	})
}

type cdcRestartDeliveries struct {
	mu        sync.Mutex
	envelopes []cdcjson.CDCMessagesEnvelope
	decodeErr error
}

func requireCDCRestartDelivery(t *testing.T, deliveries *cdcRestartDeliveries, rowID int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		deliveries.mu.Lock()
		defer deliveries.mu.Unlock()
		if deliveries.decodeErr != nil {
			return true
		}
		for _, envelope := range deliveries.envelopes {
			for _, group := range envelope.Payload {
				for _, event := range group.Events {
					if event.Table == "cdc_restart_included" && event.NewRowID == rowID {
						return true
					}
				}
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "CDC did not deliver row %d", rowID)

	deliveries.mu.Lock()
	defer deliveries.mu.Unlock()
	require.NoError(t, deliveries.decodeErr)
	// Replays may repeat committed rows; every delivery must still honor the output settings.
	for _, envelope := range deliveries.envelopes {
		require.Equal(t, "restart-source-partition-1", envelope.ServiceID)
		for _, group := range envelope.Payload {
			for _, event := range group.Events {
				require.Equal(t, "INSERT", event.Op)
				require.Equal(t, "cdc_restart_included", event.Table)
				require.Empty(t, event.Error)
				require.Nil(t, event.Before)
				require.Nil(t, event.After)
			}
		}
	}
}

func startCDCPartitionWithinDeadline(t *testing.T, fixture cdcPartitionTestFixture, settings config.CDC) (*ZenPartitionNode, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started := make(chan struct{})
	var partition *ZenPartitionNode
	var startErr error
	go func() {
		partition, startErr = fixture.start(ctx, settings)
		close(started)
	}()
	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, 10*time.Second, 10*time.Millisecond, "partition startup exceeded its deadline")

	var stopOnce sync.Once
	stopped := make(chan struct{})
	var stopErr error
	stop := func() {
		t.Helper()
		stopOnce.Do(func() {
			go func() {
				stopErr = partition.Stop()
				close(stopped)
			}()
		})
		require.Eventually(t, func() bool {
			select {
			case <-stopped:
				return true
			default:
				return false
			}
		}, 10*time.Second, 10*time.Millisecond, "partition shutdown exceeded its deadline")
		require.NoError(t, stopErr)
	}
	if partition != nil {
		t.Cleanup(stop)
	}
	require.NoError(t, startErr)
	return partition, stop
}
