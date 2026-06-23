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
		_, err := deployDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)
	})

	bpmnId := "service-task-input-output"
	var definition zenclient.ProcessDefinitionSimple
	stDefinitionCount := 0

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		response, err := deployDefinition(t, "service-task-input-output.bpmn")
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, response.StatusCode())
		assert.Nil(t, response.JSON201)
		require.NotNil(t, response.JSON200)
		assert.NotZero(t, response.JSON200.ProcessDefinitionKey)

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
	_, err := deployDefinition(t, "service-task-input-output.bpmn")
	assert.NoError(t, err)
	_, err = deployDefinition(t, "service-task-input-output-v2.bpmn")
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

	t.Run("test search filter", func(t *testing.T) {
		t.Run("search by name substring matches single result", func(t *testing.T) {
			search := "v2"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			require.Len(t, response.JSON200.Items, 1)
			assert.Equal(t, "service-task-input-output-v2", *response.JSON200.Items[0].BpmnProcessName)
		})

		t.Run("search is case-insensitive for name match", func(t *testing.T) {
			search := "V2"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			require.Len(t, response.JSON200.Items, 1)
			assert.Equal(t, "service-task-input-output-v2", *response.JSON200.Items[0].BpmnProcessName)
		})

		t.Run("search matches by bpmn_process_id as well as by name", func(t *testing.T) {
			search := "service-task-input-output"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			for _, item := range response.JSON200.Items {
				assert.True(t, strings.Contains(
					strings.ToLower(item.BpmnProcessId),
					strings.ToLower(search),
				))
			}
		})

		t.Run("search is case-insensitive for bpmn_process_id match", func(t *testing.T) {
			search := "SERVICE-TASK-INPUT-OUTPUT"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			for _, item := range response.JSON200.Items {
				assert.True(t, strings.Contains(
					strings.ToLower(item.BpmnProcessId),
					strings.ToLower(search),
				))
			}
		})

		t.Run("search substring matches multiple results", func(t *testing.T) {
			search := "service-task"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			for _, item := range response.JSON200.Items {
				assert.True(t, strings.Contains(
					strings.ToLower(item.BpmnProcessId),
					strings.ToLower(search),
				))
			}
		})

		t.Run("search returns no results for unmatched term", func(t *testing.T) {
			search := "nomatch"
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 0, response.JSON200.TotalCount)
			assert.Empty(t, response.JSON200.Items)
		})

		t.Run("search with empty string returns all definitions", func(t *testing.T) {
			search := ""
			response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Greater(t, response.JSON200.TotalCount, 0)
			assert.NotEmpty(t, response.JSON200.Items)
		})
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

func TestRestApiProcessDefinitionRedeployOlderContentCreatesNewVersion(t *testing.T) {
	processID := fmt.Sprintf("redeploy_older_content-%d", time.Now().UnixNano())
	version1Definition := versioningTestProcessDefinition(t, processID, "version 1")
	version2Definition := versioningTestProcessDefinition(t, processID, "version 2")

	version1Key := deployDefinitionFromBytesExpectingStatus(t, version1Definition, "redeploy_older_content-v1.bpmn", http.StatusCreated)
	version2Key := deployDefinitionFromBytesExpectingStatus(t, version2Definition, "redeploy_older_content-v2.bpmn", http.StatusCreated)
	redeployedVersion1Key := deployDefinitionFromBytesExpectingStatus(t, version1Definition, "redeploy_older_content-v1.bpmn", http.StatusCreated)
	duplicateLatestKey := deployDefinitionFromBytesExpectingStatus(t, version1Definition, "redeploy_older_content-v1.bpmn", http.StatusOK)

	require.NotEqual(t, version1Key, version2Key)
	require.NotEqual(t, version1Key, redeployedVersion1Key)
	require.NotEqual(t, version2Key, redeployedVersion1Key)
	require.Equal(t, redeployedVersion1Key, duplicateLatestKey)

	assertProcessDefinitionVersions(t, processID, []expectedProcessDefinitionVersion{
		{version: 1, key: version1Key},
		{version: 2, key: version2Key},
		{version: 3, key: redeployedVersion1Key},
	})
	assertLatestProcessDefinitionVersion(t, processID, 3, redeployedVersion1Key)
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

func deployDefinition(t testing.TB, filename string) (*zenclient.CreateProcessDefinitionResponse, error) {
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
	return resp, nil
}

func deployUniqueDefinition(t testing.TB, filename string) (replacedDefinitionId *string, err error) {
	t.Helper()

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
	replacedDefinitionId = ptr.To(fmt.Sprintf("%v-%v", oldDefinitionId, time.Now().UnixNano()))
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

func deployUniqueProcessDefinition(t testing.TB, filepathn string) (replacedDefinitionId *string) {
	t.Helper()

	wd, err := os.Getwd()
	require.NoError(t, err)

	loc := filepath.Join(wd, filepathn)

	return deployProcessDefinition(t, loc)
}

func deployProcessDefinition(t testing.TB, filepath string) (replacedDefinitionId *string) {
	t.Helper()

	file, err := os.ReadFile(filepath)
	require.NoError(t, err)

	stringFile := string(file)
	oldDefinitionId, found := getStringInBetweenTwoString(stringFile, "bpmn:process id=\"", "\"")

	require.True(t, found, "didn't find BPMN process id for filename %v", filepath)

	replacedDefinitionId = ptr.To(fmt.Sprintf("%v-%v", oldDefinitionId, time.Now().UnixNano()))
	fileString := strings.ReplaceAll(stringFile, "bpmn:process id=\""+oldDefinitionId+"\"", "bpmn:process id=\""+*replacedDefinitionId+"\"")
	file = []byte(fileString)

	// Create multipart form data
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)

	// Create the resource field as required by the OpenAPI spec
	part, err := writer.CreateFormFile("resource", filepath)
	require.NoError(t, err, fmt.Errorf("failed to create form file: %w", err))

	_, err = part.Write(file)
	require.NoError(t, err, fmt.Errorf("failed to write file to multipart form: %w", err))

	err = writer.Close()
	require.NoError(t, err, fmt.Errorf("failed to close multipart writer: %w", err))

	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	require.NoError(t, err, fmt.Errorf("failed to deploy process definition: %w", err))

	isErrorResponse := resp.StatusCode() >= 400
	require.False(t, isErrorResponse, "failed to deploy process definition: %s", string(resp.Body))

	definition := public.CreateProcessDefinition201JSONResponse{}
	err = json.Unmarshal(resp.Body, &definition)
	require.NoError(t, err, fmt.Errorf("failed to unmarshal create definition response: %w", err))

	return replacedDefinitionId
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

func deployDefinitionFromBytes(t testing.TB, content []byte, filename string) (*zenclient.CreateProcessDefinitionResponse, error) {
	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	part, err := writer.CreateFormFile("resource", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err = part.Write(content); err != nil {
		return nil, fmt.Errorf("failed to write file to multipart form: %w", err)
	}
	if err = writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close multipart writer: %w", err)
	}
	resp, err := app.restClient.CreateProcessDefinitionWithBodyWithResponse(t.Context(), writer.FormDataContentType(), &requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to deploy process definition: %w", err)
	}
	if resp.StatusCode() >= 400 {
		return nil, fmt.Errorf("failed to deploy process definition: %s", string(resp.Body))
	}
	return resp, nil
}

func deployDefinitionFromBytesExpectingStatus(t testing.TB, content []byte, filename string, expectedStatus int) int64 {
	t.Helper()

	resp, err := deployDefinitionFromBytes(t, content, filename)
	require.NoError(t, err)
	require.Equal(t, expectedStatus, resp.StatusCode())

	switch expectedStatus {
	case http.StatusCreated:
		require.NotNil(t, resp.JSON201)
		return resp.JSON201.ProcessDefinitionKey
	case http.StatusOK:
		require.NotNil(t, resp.JSON200)
		return resp.JSON200.ProcessDefinitionKey
	default:
		t.Fatalf("unsupported expected status %d", expectedStatus)
		return 0
	}
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

type expectedProcessDefinitionVersion struct {
	version int
	key     int64
}

func assertProcessDefinitionVersions(t testing.TB, processID string, expectedVersions []expectedProcessDefinitionVersion) {
	t.Helper()

	definitions := getProcessDefinitionVersions(t, processID, false)
	require.Len(t, definitions, len(expectedVersions))

	for i, expected := range expectedVersions {
		assert.Equal(t, expected.version, definitions[i].Version)
		assert.Equal(t, expected.key, definitions[i].Key)
	}
}

func assertLatestProcessDefinitionVersion(t testing.TB, processID string, expectedVersion int, expectedKey int64) {
	t.Helper()

	definitions := getProcessDefinitionVersions(t, processID, true)
	require.Len(t, definitions, 1)
	assert.Equal(t, expectedVersion, definitions[0].Version)
	assert.Equal(t, expectedKey, definitions[0].Key)
}

func getProcessDefinitionVersions(t testing.TB, processID string, onlyLatest bool) []zenclient.ProcessDefinitionSimple {
	t.Helper()

	sortBy := zenclient.GetProcessDefinitionsParamsSortByVersion
	sortOrder := zenclient.GetProcessDefinitionsParamsSortOrderAsc
	response, err := app.restClient.GetProcessDefinitionsWithResponse(t.Context(), &zenclient.GetProcessDefinitionsParams{
		BpmnProcessId: &processID,
		OnlyLatest:    &onlyLatest,
		SortBy:        &sortBy,
		SortOrder:     &sortOrder,
		Size:          ptr.To(int32(10)),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
	require.Equal(t, len(response.JSON200.Items), response.JSON200.TotalCount)
	return response.JSON200.Items
}

func versioningTestProcessDefinition(t testing.TB, processID string, taskName string) []byte {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "process_definition", "redeploy_older_content.bpmn"))
	require.NoError(t, err)

	definition := strings.ReplaceAll(string(data), "{{PROCESS_ID}}", processID)
	definition = strings.ReplaceAll(definition, "{{TASK_NAME}}", taskName)
	return []byte(definition)
}

func TestRestApiProcessDefinitionErrors(t *testing.T) {
	t.Run("CreateProcessDefinition - idempotent deploy returns 200 on duplicate content", func(t *testing.T) {
		// Ensure the definition is deployed at least once before testing idempotency
		first, err := deployDefinitionRaw(t, "service-task-input-output.bpmn")
		require.NoError(t, err)
		// The first deploy may be 201 (fresh) or 200 (another test deployed it already);
		// either way we need the key to compare against.
		var firstKey int64
		switch first.StatusCode() {
		case http.StatusCreated:
			require.NotNil(t, first.JSON201)
			firstKey = first.JSON201.ProcessDefinitionKey
		case http.StatusOK:
			require.NotNil(t, first.JSON200)
			firstKey = first.JSON200.ProcessDefinitionKey
		default:
			t.Fatalf("unexpected status for initial deploy: %d body=%s", first.StatusCode(), string(first.Body))
		}
		resp, err := deployDefinitionRaw(t, "service-task-input-output.bpmn")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)
		assert.Equal(t, firstKey, resp.JSON200.ProcessDefinitionKey)
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

	t.Run("CreateProcessDefinition - 400 for unsupported BPMN element type", func(t *testing.T) {
		resp, err := deployDefinitionRaw(t, "unsupported-script-task.bpmn")
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode())
		require.NotNil(t, resp.JSON400)
		assert.Equal(t, "BAD_REQUEST", resp.JSON400.Code)
		assert.Contains(t, resp.JSON400.Message, "unsupported element type")
		assert.Contains(t, resp.JSON400.Message, "scriptTask")
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
