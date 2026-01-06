package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
)

func TestRestApiProcessDefinition(t *testing.T) {
	t.Run("deploy process definition", func(t *testing.T) {
		err := deployDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)
	})

	var definition public.ProcessDefinitionSimple
	stDefinitionCount := 0

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		err := deployDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)
		defintitions, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		for _, def := range defintitions {
			if def.BpmnProcessId == "service-task-input-output" {
				definition = def
				stDefinitionCount++
			}
		}
		assert.Equal(t, 1, stDefinitionCount)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition public.ProcessDefinitionSimple
		for _, def := range list {
			if def.BpmnProcessId == "service-task-input-output" {
				deployedDefinition = def
				break
			}
		}
		assert.Equal(t, "service-task-input-output", deployedDefinition.BpmnProcessId)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)

		detail, err := getDefinitionDetail(t, definition.Key)
		assert.NoError(t, err)
		assert.Equal(t, "service-task-input-output", detail.BpmnProcessId)
		assert.NotNil(t, detail.BpmnData)
	})
}

func getDefinitionDetail(t testing.TB, key int64) (public.ProcessDefinitionDetail, error) {
	var detail public.ProcessDefinitionDetail
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/process-definitions/%d", key)).
		DoOk()
	if err != nil {
		return detail, fmt.Errorf("failed to get %d process definition detail: %w", key, err)
	}
	err = json.Unmarshal(resp, &detail)
	if err != nil {
		return detail, fmt.Errorf("failed to unmarshal %d process definition detail: %w", key, err)
	}
	return detail, nil
}

func deployDefinition(t testing.TB, filename string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := filepath.Join(wd, "pkg", "bpmn", "test-cases", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithMultipartBody(file, filename).
		DoOk()
	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return nil
		}
		return fmt.Errorf("failed to deploy process definition: %s %w", string(resp), err)
	}
	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp, &definition)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}

func listProcessDefinitions(t testing.TB) ([]public.ProcessDefinitionSimple, error) {
	respBytes, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("GET").
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to list process definitions: %w", err)
	}
	resp := public.GetProcessDefinitions200JSONResponse{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal process definitions: %w", err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list process definitions: %v %w", resp, err)
	}
	return resp.Items, nil
}
