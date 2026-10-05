package server

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/jobmanager"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/pkg/bpmn"
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobRequestErrorClassifiesWhatTheCallerCanActOn shows a request about a
// job answers the cluster error a stale leader cache calls for, and not an
// internal error, also on a node whose job server runs because it still leads
// another partition: the engine of the lost partition is missing there.
func TestJobRequestErrorClassifiesWhatTheCallerCanActOn(t *testing.T) {
	for name, tt := range map[string]struct {
		err      error
		expected zenerr.ZenErrorCode
	}{
		"a node which leads no partition": {
			err:      jobmanager.NodeIsNotALeader,
			expected: zenerr.ClusterErrorCode,
		},
		"a node which lost the job's partition but leads another one": {
			err:      fmt.Errorf("cannot fail job 42 on partition 2: %w", jobmanager.NodeIsNotALeader),
			expected: zenerr.ClusterErrorCode,
		},
		"a node which refuses mutations while the cluster is restored": {
			err:      fmt.Errorf("failed to fail job: %w", zenerr.ClusterError(fmt.Errorf("cluster restore in progress"))),
			expected: zenerr.ClusterErrorCode,
		},
		"an unknown job": {
			err:      fmt.Errorf("failed to find job: %w", storage.ErrNotFound),
			expected: zenerr.NotFoundCode,
		},
		"a refused request": {
			err:      fmt.Errorf("retries: %w", bpmn.ErrInvalidJobRequest),
			expected: zenerr.BadRequestCode,
		},
		"a job which no longer waits": {
			err:      fmt.Errorf("job 42: %w", bpmn.ErrJobInTerminalState),
			expected: zenerr.ConflictCode,
		},
		"a failure of a delivery the job was handed out again after": {
			err:      fmt.Errorf("job 42: %w", bpmn.ErrDeliverySuperseded),
			expected: zenerr.ConflictCode,
		},
		"anything else": {
			err:      fmt.Errorf("disk full"),
			expected: zenerr.TechnicalErrorCode,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, jobRequestError(42, "fail", tt.err).Code)
		})
	}
}

// TestAnIncidentWhichCannotBeResolvedAsThingsStandIsAConflict shows a
// resolution the engine refuses because of the state of the instance reaches
// the operator as a conflict carrying the way out, not as an internal error.
func TestAnIncidentWhichCannotBeResolvedAsThingsStandIsAConflict(t *testing.T) {
	refusal := fmt.Errorf("%w: the retries of job 7 no longer evaluate; resolve it with the job's retries given (POST /v1/incidents/42/resolve)", bpmn.ErrIncidentNotResolvable)

	answer := resolveIncidentError(42, refusal)

	assert.Equal(t, zenerr.ConflictCode, answer.Code)
	assert.Contains(t, answer.Error(), "POST /v1/incidents/42/resolve")
	assert.Equal(t, zenerr.NotFoundCode, resolveIncidentError(42, fmt.Errorf("incident: %w", storage.ErrNotFound)).Code)
	assert.Equal(t, zenerr.TechnicalErrorCode, resolveIncidentError(42, fmt.Errorf("disk full")).Code)
}

// TestRetriesRefusedWithAResolutionAreABadRequestCarryingTheEngineReasonAlone
// shows retries the engine refuses to give together with a resolution reach
// the operator as a bad request naming the limit, not as an internal error.
func TestRetriesRefusedWithAResolutionAreABadRequestCarryingTheEngineReasonAlone(t *testing.T) {
	reason := "retries of job 7 must be between 1 and 100 (jobs.maxRetries), got 150"
	refusal := fmt.Errorf("failed to resolve: %w", &bpmn.InvalidJobRequestError{Reason: reason})

	answer := resolveIncidentError(42, refusal)

	assert.Equal(t, zenerr.BadRequestCode, answer.Code)
	assert.Equal(t, reason, answer.Error())
}

// TestAResolutionInConflictWithTheIncidentOrItsJobIsAConflict shows a repeated
// resolution, as a client repeating it after a timeout sends, and retries for
// a job which no longer waits reach the client as a conflict, not as an
// internal error which error tracking would report.
func TestAResolutionInConflictWithTheIncidentOrItsJobIsAConflict(t *testing.T) {
	for name, err := range map[string]error{
		"an incident resolved already": fmt.Errorf("%w: incident 42 was resolved at 2026-10-02T10:00:00Z", bpmn.ErrIncidentAlreadyResolved),
		"a job which no longer waits":  fmt.Errorf("%w: cannot update the retries of job 7", bpmn.ErrJobInTerminalState),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, zenerr.ConflictCode, resolveIncidentError(42, err).Code)
		})
	}
}

// TestARetryAtWithoutRetriesIsRefusedBeforeTheEngineIsAsked shows the RPC,
// which is reachable without the REST layer, refuses a retryAt without
// retries itself instead of resolving the incident without them. The server
// has no controller, so reaching the engine would panic.
func TestARetryAtWithoutRetriesIsRefusedBeforeTheEngineIsAsked(t *testing.T) {
	server := &Server{}

	response, err := server.ResolveIncident(t.Context(), &proto.ResolveIncidentRequest{
		IncidentKey: new(int64(42)),
		RetryAt:     new(time.Now().Add(time.Hour).UnixMilli()),
	})

	require.NoError(t, err)
	require.NotNil(t, response.Error)
	answer := zenerr.ToZenError(response.Error, errors.New("resolve"))
	assert.Equal(t, zenerr.BadRequestCode, answer.Code)
	assert.Contains(t, answer.Error(), "retryAt can only be given together with retries")
}

// TestARefusedJobRequestCarriesTheEngineReasonAlone shows the leader answers a
// request the engine refused with the engine's reason only, which names the
// field and the value, and not with the wrapping of the layers in between.
func TestARefusedJobRequestCarriesTheEngineReasonAlone(t *testing.T) {
	refusal := fmt.Errorf("failed to fail job 42: %w", &bpmn.InvalidJobRequestError{Reason: "retries of job 42 must not be negative, got -1"})

	answer := jobRequestError(42, "fail", refusal)

	assert.Equal(t, zenerr.BadRequestCode, answer.Code)
	assert.Equal(t, "retries of job 42 must not be negative, got -1", answer.Error())
}
