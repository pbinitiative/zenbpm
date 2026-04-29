package e2e

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/require"
)

func TestDmnResourceDefinitionsPagination(t *testing.T) {
	t.Parallel()
	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /dmn-resource-definitions",
		Setup: func(t *testing.T) (cleanup func()) {
			createManyDmnResourceDefinitions(t, 11)
			return nil
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			resp, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(),
				&zenclient.GetDmnResourceDefinitionsParams{
					Page: ptr.To(int32(page)),
					Size: ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)
			return len(resp.JSON200.Items), resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 5,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 5},
					{Page: 2, ExpectedCount: 5},
				},
				TotalCount:     11,
				TotalCountMode: AtLeastCount,
			},
		},
	})
}

func TestDecisionInstancesPagination(t *testing.T) {
	t.Parallel()
	var dmnKey int64
	var seedTime time.Time

	RunPaginationTests(t, PaginationTestConfig{
		EndpointName: "GET /decision-instances",
		Setup: func(t *testing.T) (cleanup func()) {
			key, uniqueId, st := deployDmnAndSeed(t)
			dmnKey = key
			seedTime = st

			decisionId := uniqueId + "Rule"
			createDecisionInstances(t, uniqueId, decisionId, 11)
			waitForDecisionInstances(t, dmnKey, seedTime, 11)

			return nil
		},
		FetchPage: func(t *testing.T, page, size int) (int, int, int, int) {
			st := seedTime
			resp, err := app.restClient.GetDecisionInstancesWithResponse(t.Context(),
				&zenclient.GetDecisionInstancesParams{
					DmnResourceDefinitionKey: &dmnKey,
					EvaluatedFrom:            &st,
					Page:                     ptr.To(int32(page)),
					Size:                     ptr.To(int32(size)),
				})
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode())
			require.NotNil(t, resp.JSON200)

			count := 0
			for _, p := range resp.JSON200.Partitions {
				count += len(p.Items)
			}
			return count, resp.JSON200.TotalCount, resp.JSON200.Page, resp.JSON200.Size
		},
		Scenarios: []PageScenario{
			{
				PageSize: 5,
				Pages: []PageExpectation{
					{Page: 1, ExpectedCount: 5},
					{Page: 2, ExpectedCount: 5},
					{Page: 3, ExpectedCount: 1},
				},
				TotalCount:     11,
				TotalCountMode: ExactCount,
			},
		},
	})
}

func createManyDmnResourceDefinitions(t *testing.T, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		uniqueId := fmt.Sprintf("pagination-dmn-%d-%d", time.Now().UnixNano(), i)
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t,
			"bulk-evaluation-test/can-autoliquidate-rule.dmn",
			ptr.To(fmt.Sprintf("pagination-dmn-name-%d", i)),
			ptr.To(uniqueId),
		)
		require.NoError(t, err)
	}
}

func deployDmnAndSeed(t *testing.T) (int64, string, time.Time) {
	t.Helper()

	uniqueId := fmt.Sprintf("pagination-decision-%d", time.Now().UnixNano())
	key, err := deployDmnResourceDefinitionWithNewNameAndId(t,
		"bulk-evaluation-test/can-autoliquidate-rule.dmn",
		ptr.To(fmt.Sprintf("pagination-decision-name-%d", time.Now().UnixNano())),
		ptr.To(uniqueId),
	)
	require.NoError(t, err)

	seedTime := time.Now()
	return key, uniqueId, seedTime
}

func createDecisionInstances(t *testing.T, uniqueId, decisionId string, count int) {
	t.Helper()

	for i := 0; i < count; i++ {
		_, err := evaluateDecision(
			t,
			zenclient.EvaluateDecisionJSONBodyBindingTypeLatest,
			ptr.To(uniqueId),
			decisionId,
			nil,
			map[string]any{
				"claim.amountOfDamage": float64((i + 1) * 1000),
				"claim.insuranceType":  "MAJ",
			},
		)
		require.NoError(t, err)
	}
}

func waitForDecisionInstances(t *testing.T, dmnKey int64, seedTime time.Time, required int) {
	t.Helper()

	require.Eventually(t, func() bool {
		st := seedTime
		resp, err := app.restClient.GetDecisionInstancesWithResponse(t.Context(),
			&zenclient.GetDecisionInstancesParams{
				DmnResourceDefinitionKey: &dmnKey,
				EvaluatedFrom:            &st,
				Size:                     ptr.To(int32(100)),
			})
		if err != nil || resp.JSON200 == nil {
			return false
		}
		count := 0
		for _, p := range resp.JSON200.Partitions {
			count += len(p.Items)
		}
		return count >= required
	}, 10*time.Second, 200*time.Millisecond, "%d decision instances should be created", required)
}
