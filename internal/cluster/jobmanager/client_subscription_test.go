package jobmanager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAddJobSubRemovesFailedStreamAndAcceptsDesiredState(t *testing.T) {
	client, healthy, failed := subscriptionTestClient(t)

	err := client.addJobSub(t.Context(), "client-1", "job-a")

	require.NoError(t, err)
	assert.Contains(t, client.clientSubs["client-1"].jobTypes, JobType("job-a"))
	assert.Equal(t, []*clientNodeStream{healthy}, client.nodeStreams)
	assert.Equal(t, proto.SubscribeJobRequest_TYPE_SUBSCRIBE, healthy.stream.(*subscriptionTestStream).sent[0].GetType())
	assert.Equal(t, 1, failed.stream.(*subscriptionTestStream).closeCalls)
}

func TestRemoveJobSubRemovesFailedStreamAndAcceptsDesiredState(t *testing.T) {
	client, healthy, failed := subscriptionTestClient(t)
	client.clientSubs["client-1"].jobTypes["job-a"] = struct{}{}

	err := client.removeJobSub(t.Context(), "client-1", "job-a")

	require.NoError(t, err)
	assert.NotContains(t, client.clientSubs["client-1"].jobTypes, JobType("job-a"))
	assert.Equal(t, []*clientNodeStream{healthy}, client.nodeStreams)
	assert.Equal(t, proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE, healthy.stream.(*subscriptionTestStream).sent[0].GetType())
	assert.Equal(t, 1, failed.stream.(*subscriptionTestStream).closeCalls)
}

func TestRemoveClientReleasesLockAfterBroadcastPanic(t *testing.T) {
	client := newJobClient(t.Context(), "local-node", nil, nil)
	require.NoError(t, client.addClient(t.Context(), "client-1", make(chan Job)))
	client.nodeStreams = []*clientNodeStream{{
		stream: &subscriptionTestStream{ctx: t.Context(), panicOnSend: true},
		nodeID: "panicking-node",
	}}

	assert.Panics(t, func() {
		client.removeClient(t.Context(), "client-1")
	})
	require.True(t, client.clientMu.TryLock(), "client lock must be released while a panic unwinds")
	client.clientMu.Unlock()
}

func TestSendJobToDisconnectedClientDoesNotWaitForNodeBroadcast(t *testing.T) {
	client := newJobClient(t.Context(), "local-node", nil, nil)
	clientCtx, cancel := context.WithCancel(t.Context())
	require.NoError(t, client.addClient(clientCtx, "client-1", make(chan Job)))
	blockSend := make(chan struct{})
	sendStarted := make(chan struct{})
	client.nodeStreams = []*clientNodeStream{{
		stream: &subscriptionTestStream{
			ctx:         t.Context(),
			blockSend:   blockSend,
			sendStarted: sendStarted,
		},
		nodeID: "blocked-node",
	}}
	cancel()
	distributorReturned := make(chan struct{})

	go func() {
		client.sendJobToClient(Job{ClientID: "client-1"})
		close(distributorReturned)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-distributorReturned:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		select {
		case <-sendStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	close(blockSend)
}

func subscriptionTestClient(t *testing.T) (*jobClient, *clientNodeStream, *clientNodeStream) {
	t.Helper()
	client := newJobClient(t.Context(), "local-node", nil, nil)
	require.NoError(t, client.addClient(t.Context(), "client-1", make(chan Job)))
	healthy := &clientNodeStream{
		stream: &subscriptionTestStream{ctx: t.Context()},
		nodeID: "healthy-node",
	}
	failed := &clientNodeStream{
		stream: &subscriptionTestStream{ctx: t.Context(), sendErr: errors.New("broken stream")},
		nodeID: "failed-node",
	}
	client.nodeStreams = []*clientNodeStream{healthy, failed}
	return client, healthy, failed
}

type subscriptionTestStream struct {
	ctx         context.Context
	sendErr     error
	sent        []*proto.SubscribeJobRequest
	closeCalls  int
	panicOnSend bool
	blockSend   <-chan struct{}
	sendStarted chan struct{}
}

func (s *subscriptionTestStream) Send(req *proto.SubscribeJobRequest) error {
	if s.panicOnSend {
		panic("injected send panic")
	}
	if s.blockSend != nil {
		close(s.sendStarted)
		<-s.blockSend
	}
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, req)
	return nil
}

func (s *subscriptionTestStream) Recv() (*proto.SubscribeJobResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *subscriptionTestStream) Header() (metadata.MD, error) { return nil, nil }
func (s *subscriptionTestStream) Trailer() metadata.MD         { return nil }
func (s *subscriptionTestStream) CloseSend() error {
	s.closeCalls++
	return nil
}
func (s *subscriptionTestStream) Context() context.Context { return s.ctx }
func (s *subscriptionTestStream) SendMsg(any) error        { return nil }
func (s *subscriptionTestStream) RecvMsg(any) error        { return nil }
