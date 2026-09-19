package jobmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// TestServerRefusesStreamAfterStop shows a node stream reaching a job server
// which already lost leadership is refused instead of being kept open: such a
// stream would never receive a job nor the close message, and the client would
// keep it as its healthy stream to the partition leader forever.
func TestServerRefusesStreamAfterStop(t *testing.T) {
	loader := &testLoader{jobsToSend: []sql.Job{}, mu: &sync.RWMutex{}}
	server := newJobServer("node-1", loader, nil, DefaultLockLimits())
	ctx, cancel := context.WithCancel(t.Context())
	server.startServer(ctx)
	cancel()

	stream := &captureStream{
		ctx:        metadata.NewIncomingContext(t.Context(), metadata.Pairs(MetadataNodeID, "node-2")),
		sent:       map[ClientID]int{},
		sentByType: map[string]int{},
	}
	refused := make(chan error, 1)
	go func() { refused <- server.addNodeSubscription(stream) }()

	select {
	case err := <-refused:
		require.ErrorIs(t, err, NodeIsNotALeader)
	case <-time.After(2 * time.Second):
		t.Fatal("a stream reaching a stopped job server must be refused, not kept open")
	}
	server.nodeMu.RLock()
	defer server.nodeMu.RUnlock()
	assert.Empty(t, server.nodeSubs, "a refused stream is not registered")
}
