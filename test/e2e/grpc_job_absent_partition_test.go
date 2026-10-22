package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient/proto"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestJobStreamRefusesAKeyNamingAnAbsentPartitionForGood sends a completion, a
// failure and a lock extension for keys whose partition the cluster does not
// have. Such a key can never name a job, so the stream answers with a code a
// worker does not retry, not with LEADER_UNAVAILABLE, which asks for a retry.
func TestJobStreamRefusesAKeyNamingAnAbsentPartitionForGood(t *testing.T) {
	keys := map[string]int64{
		"partition 0, reserved for global resources": 1,
		"a partition the cluster does not have":      int64(1000) << int64(zenflake.StepBits),
	}
	for name, key := range keys {
		t.Run(name, func(t *testing.T) {
			stream := openRawJobStream(t)

			complete := refusalOf(t, stream, &proto.JobStreamRequest{Request: &proto.JobStreamRequest_Complete{
				Complete: &proto.JobCompleteRequest{Key: &key},
			}}, key)
			require.NotNil(t, complete, "the completion must be refused, not carried out")
			assert.Equal(t, uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND), complete.GetCode(), "complete: %s", complete.GetMessage())

			fail := refusalOf(t, stream, &proto.JobStreamRequest{Request: &proto.JobStreamRequest_Fail{
				Fail: &proto.JobFailRequest{Key: &key, Message: new("down")},
			}}, key)
			require.NotNil(t, fail, "the failure must be refused, not carried out")
			assert.Equal(t, uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_JOB_NOT_FOUND), fail.GetCode(), "fail: %s", fail.GetMessage())

			require.NoError(t, stream.Send(&proto.JobStreamRequest{Request: &proto.JobStreamRequest_ExtendLock{
				ExtendLock: &proto.JobExtendLockRequest{Key: &key},
			}}))
			extend, err := stream.Recv()
			require.NoError(t, err)
			assert.Equal(t, key, extend.GetLockExtended().GetKey())
			assert.Equal(t, uint32(proto.JobStreamErrorCode_JOB_STREAM_ERROR_CODE_LOCK_NOT_HELD), extend.GetError().GetCode(), "extend: %s", extend.GetError().GetMessage())
		})
	}
}

// openRawJobStream opens a job stream without the Go client, which only
// answers for the jobs it was handed, so a test can send requests for any key.
// The stream subscribes to no job type, so all it receives are the answers.
// It ends with the test, or after 30 seconds, whichever comes first.
func openRawJobStream(t *testing.T) grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse] {
	t.Helper()
	conn, err := grpc.NewClient(app.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	stream, err := proto.NewZenBpmClient(conn).JobStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stream.CloseSend())
		cancel()
		assert.NoError(t, conn.Close())
	})
	return stream
}

// refusalOf sends a completion or failure of the job and returns the error it
// was answered with, nil when it was carried out. A request carried out is not
// answered, so a lock extension of the job follows it: the stream answers
// requests in order, and the answer to the extension closes the answers to the
// request.
func refusalOf(t *testing.T, stream grpc.BidiStreamingClient[proto.JobStreamRequest, proto.JobStreamResponse], request *proto.JobStreamRequest, jobKey int64) *proto.ErrorResult {
	t.Helper()
	require.NoError(t, stream.Send(request))
	require.NoError(t, stream.Send(&proto.JobStreamRequest{Request: &proto.JobStreamRequest_ExtendLock{
		ExtendLock: &proto.JobExtendLockRequest{Key: &jobKey},
	}}))
	var refusal *proto.ErrorResult
	for {
		response, err := stream.Recv()
		require.NoError(t, err)
		if response.GetLockExtended() != nil {
			return refusal
		}
		if response.GetJob().GetKey() == jobKey {
			refusal = response.GetError()
		}
	}
}
