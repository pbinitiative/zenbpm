package e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestCpuProfiling(t *testing.T) {
	_, err := startCpuProfiler(t, "test-node-1")
	assert.NoError(t, err)

	//time.Sleep(5 * time.Second)

	cpuProfiler, err := stopCpuProfiler(t, "test-node-1")
	assert.NoError(t, err)
	assert.NotEmpty(t, cpuProfiler.Pprof)

	//f, err := os.Create("cpu.pprof")
	//assert.NoError(t, err)
	//defer f.Close()
	//
	//_, err = f.Write(*cpuProfiler.Pprof)
	//assert.NoError(t, err)
}

func startCpuProfiler(t testing.TB, nodeId string) (public.TestStartCpuProfile200Response, error) {
	result := public.TestStartCpuProfile200Response{}

	_, _, resp, err := app.NewRequest(t).
		WithPath("/v1/tests/" + nodeId + "/start-cpu-profile").
		WithMethod("POST").
		Do()
	if err != nil {
		return result, fmt.Errorf("failed to start cpu profiler: %w", err)
	}
	assert.Equal(t, 200, resp.StatusCode)
	return result, nil
}

func stopCpuProfiler(t testing.TB, nodeId string) (public.TestStopCpuProfile200JSONResponse, error) {
	result := public.TestStopCpuProfile200JSONResponse{}

	resp, err := app.NewRequest(t).
		WithPath("/v1/tests/"+nodeId+"/stop-cpu-profile").
		WithMethod("POST").
		WithHeader("Content-Type", "application/json").
		DoOk()
	if err != nil {
		return result, fmt.Errorf("failed to stop cpu profiler: %s %w", string(resp), err)
	}
	err = json.Unmarshal(resp, &result)
	if err != nil {
		return result, fmt.Errorf("failed to unmarshal stop cpu profiler response: %w", err)
	}
	return result, nil
}
