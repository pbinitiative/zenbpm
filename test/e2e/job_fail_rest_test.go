package e2e

import (
	"net/http"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/pbinitiative/zenbpm/pkg/zenflake"
	"github.com/stretchr/testify/require"
)

func TestRestJobFailErrorResponse(t *testing.T) {
	t.Run("job not found should return 404", func(t *testing.T) {
		partitionId := int64(1)
		nonExistingJobKey := partitionId << int64(zenflake.StepBits)

		body := zenclient.FailJobJSONRequestBody{}

		response, err := app.restClient.FailJobWithResponse(t.Context(), nonExistingJobKey, body)

		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, response.StatusCode(), "unexpected fail job response: %s body: %s", response.Status(), string(response.Body))
		require.NotNil(t, response.JSON404)
		require.Equal(t, "NOT_FOUND", response.JSON404.Code)
		require.Contains(t, response.JSON404.Message, "not found")
		require.Nil(t, response.JSON400)
		require.Nil(t, response.JSON502)
	})
}
