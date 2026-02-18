package e2e

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCpuProfiling(t *testing.T) {
	resp, err := StartPprofServer(t, "test-node-1")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	resp, err = StartPprofServer(t, "test-node-1")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)

	resp, err = StopPprofServer(t, "test-node-1")
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	resp, err = StopPprofServer(t, "test-node-1")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)
}

func StartPprofServer(t testing.TB, nodeId string) (*http.Response, error) {
	_, _, resp, err := app.NewRequest(t).
		WithPath("/v1/tests/" + nodeId + "/start-pprof-server").
		WithMethod("POST").
		Do()
	if err != nil {
		return resp, fmt.Errorf("failed to start cpu profiler: %w", err)
	}
	return resp, nil
}

func StopPprofServer(t testing.TB, nodeId string) (*http.Response, error) {
	_, _, resp, err := app.NewRequest(t).
		WithPath("/v1/tests/" + nodeId + "/stop-pprof-server").
		WithMethod("POST").
		Do()
	if err != nil {
		return resp, fmt.Errorf("failed to stop cpu profiler: %w", err)
	}
	return resp, nil
}
