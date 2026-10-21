//go:build cluster_e2e

package cluster

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// keyOfPartitionZero names partition 0, which is reserved for global
// resources: it can never name a job.
const keyOfPartitionZero int64 = 1

// TestAbsentPartitionKeyIsNotFoundOnlyOnceThePartitionsAreFormed asks nodes
// about a key of a partition the cluster does not have. While a node's state
// does not hold every desired partition yet, it cannot tell that key from one
// of a partition about to be formed, and must answer with the retryable 502; a
// 404 is only right once the partitions are formed, and then stays. Each
// answer is checked against the node's status read right after it: a node is
// never unformed again once formed, so a status still unformed shows the node
// was unformed when it answered.
func TestAbsentPartitionKeyIsNotFoundOnlyOnceThePartitionsAreFormed(t *testing.T) {
	t.Run("a cluster which is starting", func(t *testing.T) {
		tc := NewTestCluster(t, 3)
		defer tc.Teardown(t)

		for _, n := range tc.Nodes {
			answers := map[string]int{}
			require.Eventually(t, func() bool {
				status, formed, err := askAboutPartitionZero(n)
				if err != nil {
					return false
				}
				answers[fmt.Sprintf("%d formed=%t", status, formed)]++
				return status == http.StatusNotFound && formed
			}, 150*time.Second, 20*time.Millisecond, "node %s must refuse the key for good once its partitions are formed", n.ID)

			// how many answers came before formation depends on timing
			t.Logf("node %s answered the key of partition 0 with %v", n.ID, answers)
			for answer := range answers {
				assert.Contains(t, []string{"502 formed=false", "502 formed=true", "404 formed=true"}, answer, "node %s", n.ID)
			}
			status, formed, err := askAboutPartitionZero(n)
			require.NoError(t, err)
			assert.True(t, formed)
			assert.Equal(t, http.StatusNotFound, status, "node %s: formed partitions are never unformed, so the answer stays", n.ID)
		}
	})

	t.Run("a cluster which has not formed all its partitions", func(t *testing.T) {
		// the second partition never forms, as long as partition membership is
		// not implemented (see config.Cluster.ValidateDesiredPartitions)
		tc := NewTestCluster(t, 3, WithPartitions(2))
		defer tc.Teardown(t)
		require.Eventually(t, func() bool {
			for _, n := range tc.RunningNodes() {
				status, err := getStatus(n)
				if err != nil || status.Partitions["1"].LeaderID == "" {
					return false
				}
			}
			return true
		}, 60*time.Second, 100*time.Millisecond, "every node must see the first partition formed")

		for _, n := range tc.RunningNodes() {
			for range 10 {
				status, formed, err := askAboutPartitionZero(n)
				require.NoError(t, err)
				if formed {
					t.Skip("the second partition formed; this case needs a cluster which has not formed all its partitions")
				}
				assert.Equal(t, http.StatusBadGateway, status, "node %s has not formed its partitions, so it cannot tell the key from one of a partition about to be formed", n.ID)
			}
		}
	})
}

// TestJobOfAnExistingPartitionIsNeverNotFoundDuringPartitionLeaderFailover
// kills the leader of the partition and completes two of its jobs through a
// surviving node until both completions are confirmed, one over REST and one
// over the job stream. While the partition has no leader, a completion may
// fail with what the worker is to retry, but never with not found: the
// partition exists, it only lacks a leader. A key of a partition the cluster
// does not have stays not found throughout, whether a leader is known or not.
func TestJobOfAnExistingPartitionIsNeverNotFoundDuringPartitionLeaderFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Teardown(t)
	WaitForHealthy(t, tc, 150*time.Second)
	WaitForPartitions(t, tc, 1, 30*time.Second)

	leader := tc.Leader()
	require.NotNil(t, leader)
	DeployDefinitionOnNode(t, leader, "simple_task.bpmn")
	definitionKey := GetFirstDefinitionKey(t, leader)
	restJobKey := activeJobOf(t, leader, CreateInstanceOnNode(t, leader, definitionKey, nil))
	streamJobKey := activeJobOf(t, leader, CreateInstanceOnNode(t, leader, definitionKey, nil))

	partitionLeaderID := partitionLeaderOf(t, leader, 1)
	var survivor *TestNode
	for _, n := range tc.RunningNodes() {
		if n.ID != partitionLeaderID {
			survivor = n
			break
		}
	}
	require.NotNil(t, survivor)
	stream, closeStream := openRawJobStream(t, survivor)
	defer closeStream()

	tc.KillNode(t, partitionLeaderID)

	var notFound []string
	// retried counts the answers to retry by kind, lastRetried keeps the
	// latest message of each kind for the log
	retried, lastRetried := map[string]int{}, map[string]string{}
	restCompleted, streamCompleted := false, false
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if absent, err := survivor.RestClient.GetJobWithResponse(ctx, keyOfPartitionZero); err == nil && absent.StatusCode() != http.StatusNotFound {
			notFound = append(notFound, fmt.Sprintf("the key of partition 0 answered %d, not 404: %s", absent.StatusCode(), string(absent.Body)))
		}
		if !restCompleted {
			response, err := survivor.RestClient.CompleteJobWithResponse(ctx, restJobKey, zenclient.CompleteJobJSONRequestBody{})
			switch {
			case err != nil:
				retried["REST transport error"]++
				lastRetried["REST transport error"] = err.Error()
			case response.StatusCode() == http.StatusCreated:
				restCompleted = true
			case response.StatusCode() == http.StatusNotFound:
				notFound = append(notFound, fmt.Sprintf("REST completion answered 404: %s", string(response.Body)))
			default:
				kind := fmt.Sprintf("REST %d", response.StatusCode())
				retried[kind]++
				lastRetried[kind] = string(response.Body)
			}
		}
		if !streamCompleted {
			refusal, err := completeOverStream(stream, streamJobKey)
			switch {
			case err != nil:
				retried["stream error"]++
				lastRetried["stream error"] = err.Error()
			case refusal == nil:
				streamCompleted = true
			case refusal.GetCode() == uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND):
				notFound = append(notFound, fmt.Sprintf("stream completion answered JOB_NOT_FOUND: %s", refusal.GetMessage()))
			default:
				kind := fmt.Sprintf("stream code %d", refusal.GetCode())
				retried[kind]++
				lastRetried[kind] = refusal.GetMessage()
			}
		}
		return restCompleted && streamCompleted
	}, 120*time.Second, 200*time.Millisecond, "both completions must be confirmed once the partition has a new leader")

	assert.Empty(t, notFound, "a job of a partition without a leader is not unknown")
	t.Logf("answers retried before the completions were confirmed: %v; the latest of each: %v", retried, lastRetried)
}

// askAboutPartitionZero reads the job with the key of partition 0 from the
// node, and then whether the node's state holds every desired partition.
func askAboutPartitionZero(n *TestNode) (int, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := n.RestClient.GetJobWithResponse(ctx, keyOfPartitionZero)
	if err != nil {
		return 0, false, err
	}
	status, err := getStatus(n)
	if err != nil {
		return 0, false, err
	}
	desired := status.ClusterConfig.DesiredPartitions
	return response.StatusCode(), desired > 0 && int64(len(status.Partitions)) == desired, nil
}

// partitionLeaderOf returns the id of the node n reports as the leader of the partition.
func partitionLeaderOf(t *testing.T, n *TestNode, partitionID int64) string {
	t.Helper()
	status, err := getStatus(n)
	require.NoError(t, err)
	for _, partition := range status.Partitions {
		if partition.ID == partitionID && partition.LeaderID != "" {
			return partition.LeaderID
		}
	}
	require.Failf(t, "no partition leader", "node %s reports no leader of partition %d", n.ID, partitionID)
	return ""
}

// openRawJobStream opens a job stream to the node without the Go client, which
// only answers for the jobs it was handed, so a test can send requests for any
// key. It subscribes to no job type, so all it receives are the answers.
func openRawJobStream(t *testing.T, n *TestNode) (grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], func()) {
	t.Helper()
	conn, err := grpc.NewClient(n.GrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	stream, err := proto.NewZenBpmClient(conn).JobStream(ctx)
	require.NoError(t, err)
	return stream, func() {
		assert.NoError(t, stream.CloseSend())
		cancel()
		assert.NoError(t, conn.Close())
	}
}

// completeOverStream completes the job over the stream and returns the error
// the completion was answered with, nil when it was carried out. A carried-out
// completion is not answered, so a lock extension of the job follows it: the
// stream answers requests in order, and the answer to the extension, whatever
// it says, closes the answers to the completion.
func completeOverStream(stream grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], jobKey int64) (*proto.ErrorResult, error) {
	if err := stream.Send(&proto.JobStreamRequest{Request: &proto.JobStreamRequest_Complete{
		Complete: &proto.JobCompleteRequest{Key: &jobKey},
	}}); err != nil {
		return nil, fmt.Errorf("failed to send completion: %w", err)
	}
	if err := stream.Send(&proto.JobStreamRequest{Request: &proto.JobStreamRequest_ExtendLock{
		ExtendLock: &proto.JobExtendLockRequest{Key: &jobKey},
	}}); err != nil {
		return nil, fmt.Errorf("failed to send lock extension: %w", err)
	}
	var refusal *proto.ErrorResult
	for {
		response, err := stream.Recv()
		if err != nil {
			return nil, fmt.Errorf("failed to receive: %w", err)
		}
		if response.GetLockExtended() != nil {
			return refusal, nil
		}
		if response.GetJob().GetKey() == jobKey && response.GetError() != nil {
			refusal = response.GetError()
		}
	}
}
