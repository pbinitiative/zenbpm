package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestApiProcessDefinition(t *testing.T) {
	t.Run("deploy process definition", func(t *testing.T) {
		err := deployDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)
	})

	bpmnId := "service-task-input-output"
	var definition zenclient.ProcessDefinitionSimple
	stDefinitionCount := 0

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		err := deployDefinition(t, "service-task-input-output.bpmn")
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

	// Deploy data for filtering and sorting testing in the subtests
	err := deployDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	err = deployDefinition(t, "service-task-input-output-v2.bpmn")
	assert.NoError(t, err)

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listProcessDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition zenclient.ProcessDefinitionSimple
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

	t.Run("test latest version filter", func(t *testing.T) {
		onlyLatest := false

		response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
			BpmnProcessId: &bpmnId,
			OnlyLatest:    &onlyLatest,
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, response.JSON200.TotalCount)

		onlyLatest = true
		response, err = app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
			BpmnProcessId: &bpmnId,
			OnlyLatest:    &onlyLatest,
		})

		assert.NoError(t, err)
		assert.Equal(t, 1, response.JSON200.TotalCount)
	})

	t.Run("test sorting", func(t *testing.T) {
		onlyLatest := false
		sortBy := zenclient.GetProcessDefinitionsParamsSortByVersion
		sortOrder := zenclient.GetProcessDefinitionsParamsSortOrderDesc

		response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
			BpmnProcessId: &bpmnId,
			OnlyLatest:    &onlyLatest,
			SortBy:        &sortBy,
			SortOrder:     &sortOrder,
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, response.JSON200.TotalCount)
		assert.Greater(t, response.JSON200.Items[0].Version, response.JSON200.Items[1].Version)

		sortOrder = zenclient.GetProcessDefinitionsParamsSortOrderAsc

		response, err = app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
			BpmnProcessId: &bpmnId,
			OnlyLatest:    &onlyLatest,
			SortBy:        &sortBy,
			SortOrder:     &sortOrder,
		})

		assert.NoError(t, err)
		assert.Equal(t, 2, response.JSON200.TotalCount)
		assert.Less(t, response.JSON200.Items[0].Version, response.JSON200.Items[1].Version)
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

	// Create multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Create the resource field as required by the OpenAPI spec
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	_, err = part.Write(file)
	if err != nil {
		return fmt.Errorf("failed to write file to multipart form: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	if err != nil {
		return fmt.Errorf("failed to deploy process definition: %w", err)
	}

	if resp.StatusCode() >= 400 && resp.StatusCode() != http.StatusConflict {
		return fmt.Errorf("failed to deploy process definition: %s", string(resp.Body))
	}

	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp.Body, &definition)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}

func deployUniqueDefinition(t testing.TB, filename string) (replacedDefinitionId *string, err error) {
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
	stringFile := string(file)
	oldDefinitionId, found := getStringInBetweenTwoString(stringFile, "bpmn:process id=\"", "\"")
	if !found {
		return nil, fmt.Errorf("didn't find bpmn process id for filename %v", filename)
	}
	replacedDefinitionId = ptr.To(fmt.Sprintf("%v-%v", oldDefinitionId, time.Now().UnixMilli()))
	fileString := strings.ReplaceAll(stringFile, "bpmn:process id=\""+oldDefinitionId+"\"", "bpmn:process id=\""+*replacedDefinitionId+"\"")
	file = []byte(fileString)

	// Create multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Create the resource field as required by the OpenAPI spec
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}

	_, err = part.Write(file)
	if err != nil {
		return nil, fmt.Errorf("failed to write file to multipart form: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to deploy process definition: %w", err)
	}

	if resp.StatusCode() >= 400 {
		return nil, fmt.Errorf("failed to deploy process definition: %s", string(resp.Body))
	}

	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp.Body, &definition)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return replacedDefinitionId, nil
}

func listProcessDefinitions(t testing.TB) ([]zenclient.ProcessDefinitionSimple, error) {

	resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
		Size: ptr.To(int32(100)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list process definitions: %w", err)
	}
	return resp.JSON200.Items, nil
}

func deployDefinitionRaw(t testing.TB, filename string) (*zenclient.CreateProcessDefinitionResponse, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	file, err := os.ReadFile(filepath.Join(wd, "pkg", "bpmn", "test-cases", filename))
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err = part.Write(file); err != nil {
		return nil, fmt.Errorf("failed to write file to multipart form: %w", err)
	}
	if err = writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}

	return app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
}

func TestRestApiProcessDefinitionErrors(t *testing.T) {
	t.Run("CreateProcessDefinition - 409 conflict on duplicate content", func(t *testing.T) {
		// Ensure the definition is deployed at least once before testing conflict
		_, err := deployDefinitionRaw(t, "service-task-input-output.bpmn")
		require.NoError(t, err)
		// Second deployment of identical content must always return 409
		resp, err := deployDefinitionRaw(t, "service-task-input-output.bpmn")
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, resp.StatusCode())
		require.NotNil(t, resp.JSON409)
		assert.Equal(t, "CONFLICT", resp.JSON409.Code)
	})

	t.Run("GetProcessDefinition - 404 for nonexistent key", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionWithResponse(t.Context(), int64(999999999))
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode())
		require.NotNil(t, resp.JSON404)
		assert.Equal(t, "NOT_FOUND", resp.JSON404.Code)
	})

	t.Run("GetProcessDefinition - 400 for key 0", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionWithResponse(t.Context(), int64(0))
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})

	t.Run("GetProcessDefinitions - 400 for page=0", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionsParams{
				Page: ptr.To(int32(0)),
				Size: ptr.To(int32(10)),
			})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})

	t.Run("GetProcessDefinitions - 400 for size=0", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionsParams{
				Page: ptr.To(int32(1)),
				Size: ptr.To(int32(0)),
			})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})

	t.Run("GetProcessDefinitions - 400 for size>100", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(),
			&zenclient.GetProcessDefinitionsParams{
				Page: ptr.To(int32(1)),
				Size: ptr.To(int32(101)),
			})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})

	t.Run("GetProcessDefinitionElementStatistics - 404 for nonexistent key", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), int64(999999999))
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode())
		require.NotNil(t, resp.JSON404)
		assert.Equal(t, "NOT_FOUND", resp.JSON404.Code)
	})

	t.Run("GetProcessDefinitionElementStatistics - 400 for key 0", func(t *testing.T) {
		resp, err := app.restClient.GetProcessDefinitionElementStatisticsWithResponse(t.Context(), int64(0))
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
	})
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
