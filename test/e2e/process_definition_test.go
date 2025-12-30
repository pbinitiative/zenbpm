package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/stretchr/testify/assert"
)

func TestRestApiProcessDefinition(t *testing.T) {
	t.Run("deploy process definition", func(t *testing.T) {
		_, err := deployDefinition(t, "service-task-input-output.bpmn", false)
		assert.NoError(t, err)
	})

	var definition public.ProcessDefinitionSimple
	stDefinitionCount := 0

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		_, err := deployDefinition(t, "service-task-input-output.bpmn", false)
		assert.NoError(t, err)
		definitions, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		for _, def := range definitions {
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

func deployDefinition(t testing.TB, filename string, isReplace bool) (replacedDefinitionId *string, err error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := filepath.Join(wd, "pkg", "bpmn", "test-cases", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	if isReplace {
		stringFile := string(file)
		oldDefinitionId, found := getStringInBetweenTwoString(stringFile, "bpmn:process id=\"", "\"")
		if !found {
			return nil, fmt.Errorf("didn't find bpmn process id for filename %v", filename)
		}
		replacedDefinitionId = ptr.To(fmt.Sprintf("%v-%v", oldDefinitionId, time.Now().UnixMilli()))
		fileString := strings.ReplaceAll(stringFile, "bpmn:process id=\""+oldDefinitionId+"\"", "bpmn:process id=\""+*replacedDefinitionId+"\"")
		file = []byte(fileString)
	}

	resp, err := app.NewRequest(t).
		WithPath("/v1/process-definitions").
		WithMethod("POST").
		WithMultipartBody(file, filename).
		DoOk()
	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to deploy process definition: %s %w", string(resp), err)
	}
	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp, &definition)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return replacedDefinitionId, nil
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
	return resp.Items, nil
}

func getStringInBetweenTwoString(str string, startS string, endS string) (result string, found bool) {
	s := strings.Index(str, startS)
	if s == -1 {
		return result, false
	}
	newS := str[s+len(startS):]
	e := strings.Index(newS, endS)
	if e == -1 {
		return result, false
	}
	result = newS[:e]
	return result, true
}
