package jobmanager

import (
	"context"
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestAddJobSubRemovesFailedStreamAndKeepsDesiredState(t *testing.T) {
	client, healthy, failed := subscriptionTestClient(t)

	err := client.addJobSub(t.Context(), "client-1", "job-a")

	require.ErrorContains(t, err, "failed-node")
	assert.Contains(t, client.clientSubs["client-1"].jobTypes, JobType("job-a"))
	assert.Equal(t, []*clientNodeStream{healthy}, client.nodeStreams)
	assert.Equal(t, proto.SubscribeJobRequest_TYPE_SUBSCRIBE, healthy.stream.(*subscriptionTestStream).sent[0].GetType())
	assert.Equal(t, 1, failed.stream.(*subscriptionTestStream).closeCalls)
}

func TestRemoveJobSubRemovesFailedStreamAndKeepsDesiredState(t *testing.T) {
	client, healthy, failed := subscriptionTestClient(t)
	client.clientSubs["client-1"].jobTypes["job-a"] = struct{}{}

	err := client.removeJobSub(t.Context(), "client-1", "job-a")

	require.ErrorContains(t, err, "failed-node")
	assert.NotContains(t, client.clientSubs["client-1"].jobTypes, JobType("job-a"))
	assert.Equal(t, []*clientNodeStream{healthy}, client.nodeStreams)
	assert.Equal(t, proto.SubscribeJobRequest_TYPE_UNSUBSCRIBE, healthy.stream.(*subscriptionTestStream).sent[0].GetType())
	assert.Equal(t, 1, failed.stream.(*subscriptionTestStream).closeCalls)
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
	ctx        context.Context
	sendErr    error
	sent       []*proto.SubscribeJobRequest
	closeCalls int
}

func (s *subscriptionTestStream) Send(req *proto.SubscribeJobRequest) error {
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
