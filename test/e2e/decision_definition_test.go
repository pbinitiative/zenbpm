package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestApiDmnResourceDefinition(t *testing.T) {
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		name := uniqueDmnResourceDefinitionTestValue("restDeployName")
		id := uniqueDmnResourceDefinitionTestValue("restDeployId")
		response, err := deployDmnResourceDefinitionWithNewNameAndIdResponse(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name, &id)
		assert.NoError(t, err)
		assert.NotNil(t, response.JSON201)
	})

	t.Run("DMN resource definition should not throw panic if deploying a second DMN with the same decisionTable Id", func(t *testing.T) {
		response, err := deployDmnResourceDefinition(t, "definition/dmn_deployment_same_table_id_1.dmn")
		assert.NoError(t, err)
		assert.NotNil(t, response.JSON201)

		response, err = deployDmnResourceDefinition(t, "definition/dmn_deployment_same_table_id_2.dmn")
		assert.NoError(t, err)
		assert.NotNil(t, response.JSON201)
	})

	t.Run("repeatedly calling rest api to deploy the same definition would return conflict response", func(t *testing.T) {
		name := uniqueDmnResourceDefinitionTestValue("restRepeatedName")
		id := uniqueDmnResourceDefinitionTestValue("restRepeatedId")
		firstResponse, err := deployDmnResourceDefinitionWithNewNameAndIdResponse(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name, &id)
		assert.NoError(t, err)
		assert.NotNil(t, firstResponse.JSON201)

		response, err := deployDmnResourceDefinitionWithNewNameAndIdResponse(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name, &id)
		assert.NoError(t, err)
		assert.Nil(t, response.JSON201)
		assert.NotNil(t, response.JSON200)
		assert.NotZero(t, response.JSON200.DmnResourceDefinitionKey)

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &id,
		})
		assert.NoError(t, err)
		count := 0
		for _, def := range definitions {
			if def.DmnResourceDefinitionId == id {
				count++
			}
		}
		assert.Equal(t, 1, count)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		name := uniqueDmnResourceDefinitionTestValue("restListName")
		id := uniqueDmnResourceDefinitionTestValue("restListId")
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name, &id)
		assert.NoError(t, err)

		list, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &id,
		})
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition zenclient.DmnResourceDefinitionSimple
		for _, def := range list {
			if def.DmnResourceDefinitionId == id {
				deployedDefinition = def
				break
			}
		}
		assert.Equal(t, id, deployedDefinition.DmnResourceDefinitionId)
	})

	t.Run("get single deployed definition", func(t *testing.T) {
		name := uniqueDmnResourceDefinitionTestValue("singleDeployment")
		id := uniqueDmnResourceDefinitionTestValue("singleDeploymentId")
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name, &id)
		assert.NoError(t, err)

		list, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &id,
		})
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)

		var found *zenclient.DmnResourceDefinitionSimple

		for i := range list {
			if list[i].DmnDefinitionName == name &&
				list[i].DmnResourceDefinitionId == id {
				found = &list[i]
				break
			}
		}
		require.NotNil(t, found)

		detail, err := app.restClient.GetDmnResourceDefinitionWithResponse(t.Context(), found.Key)
		assert.NoError(t, err)
		assert.Equal(t, found.DmnResourceDefinitionId, detail.JSON200.DmnResourceDefinitionId)
		assert.Equal(t, found.DmnDefinitionName, detail.JSON200.DmnDefinitionName)
		assert.NotNil(t, detail.JSON200.DmnData)
	})

	t.Run("getting dmn resource definition with non existing key would return NOT_FOUND", func(t *testing.T) {
		var nonExistingDmnResourceDefinitionKey int64 = -1
		response, _ := app.restClient.GetDmnResourceDefinitionWithResponse(t.Context(), nonExistingDmnResourceDefinitionKey)
		assert.Nil(t, response.JSON200)
		assert.NotNil(t, response.JSON404)
		assert.Equal(t, "NOT_FOUND", response.JSON404.Code)
		assert.Equal(t, "dmn resource definition by key -1 not found", response.JSON404.Message)
	})
}

func TestGetDmnResourceDefinitions(t *testing.T) {
	prefix := uniqueDmnResourceDefinitionTestValue("dmnDefinitionSearch")
	name11 := prefix + "Name11"
	name12 := prefix + "Name12"
	name21 := prefix + "Name21"
	name31 := prefix + "Name31"
	jmeno41 := prefix + "Jmeno41"
	defId1 := prefix + "DefId1"
	defId2 := prefix + "DefId2"
	defId3 := prefix + "DefId3"
	defId4 := prefix + "DefId4"

	t.Run("deploy dmn resource definition", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name11, &defId1)
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name12, &defId1)
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name21, &defId2)
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &name31, &defId3)
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", &jmeno41, &defId4)
		assert.NoError(t, err)
	})

	t.Run("find dmn resource definition by name sub string case insensitive, onlyLatest=true, order by name desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			DmnDefinitionName: new(strings.ToUpper(prefix + "name")),
			OnlyLatest:        new(true),
			SortBy:            new(zenclient.GetDmnResourceDefinitionsParamsSortByDmnDefinitionName),
			SortOrder:         new(zenclient.GetDmnResourceDefinitionsParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, processInstances.JSON200.TotalCount)
		assert.Equal(t, name31, processInstances.JSON200.Items[0].DmnDefinitionName)
		assert.Equal(t, name21, processInstances.JSON200.Items[1].DmnDefinitionName)
		assert.Equal(t, name12, processInstances.JSON200.Items[2].DmnDefinitionName)
	})

	t.Run("find dmn resource definition by dmnResourceDefinitionId, order by name asc", func(t *testing.T) {
		processInstances, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: &defId1,
			SortBy:                  new(zenclient.GetDmnResourceDefinitionsParamsSortByDmnDefinitionName),
			SortOrder:               new(zenclient.GetDmnResourceDefinitionsParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		assert.Equal(t, name11, processInstances.JSON200.Items[0].DmnDefinitionName)
		assert.Equal(t, defId1, processInstances.JSON200.Items[0].DmnResourceDefinitionId)
		assert.Equal(t, name12, processInstances.JSON200.Items[1].DmnDefinitionName)
		assert.Equal(t, defId1, processInstances.JSON200.Items[1].DmnResourceDefinitionId)
	})

	t.Run("find dmn resource definition by search across id and name", func(t *testing.T) {
		t.Run("search by exact id matches single result", func(t *testing.T) {
			search := defId2
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			assert.Equal(t, defId2, response.JSON200.Items[0].DmnResourceDefinitionId)
		})

		t.Run("search by exact name matches single result", func(t *testing.T) {
			search := jmeno41
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			assert.Equal(t, jmeno41, response.JSON200.Items[0].DmnDefinitionName)
		})

		t.Run("search is case-insensitive across id", func(t *testing.T) {
			search := strings.ToUpper(defId2)
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			assert.Equal(t, defId2, response.JSON200.Items[0].DmnResourceDefinitionId)
		})

		t.Run("search is case-insensitive across name", func(t *testing.T) {
			search := strings.ToUpper(jmeno41)
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, response.JSON200.TotalCount)
			assert.Equal(t, jmeno41, response.JSON200.Items[0].DmnDefinitionName)
		})

		t.Run("search substring matches multiple results", func(t *testing.T) {
			search := prefix + "Name"
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 4, response.JSON200.TotalCount)
			for _, item := range response.JSON200.Items {
				assert.Contains(t, strings.ToLower(item.DmnDefinitionName)+strings.ToLower(item.DmnResourceDefinitionId), strings.ToLower(search))
			}
		})

		t.Run("search returns no results for unmatched term", func(t *testing.T) {
			search := prefix + "NoMatch"
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Equal(t, 0, response.JSON200.TotalCount)
			assert.Empty(t, response.JSON200.Items)
		})

		t.Run("search with empty string returns all definitions", func(t *testing.T) {
			search := ""
			response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
				Search: &search,
			})
			assert.NoError(t, err)
			assert.Greater(t, response.JSON200.TotalCount, 0)
			assert.NotEmpty(t, response.JSON200.Items)
		})
	})
}

func TestGetDmnResourceDefinitionsBadRequests(t *testing.T) {
	t.Run("GetDmnResourceDefinitions. Provide wrong sortBy, expect Bad Request", func(t *testing.T) {
		dmnResourceDefinitions, _ := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			SortBy: (*zenclient.GetDmnResourceDefinitionsParamsSortBy)(new("unsupportedSortColumn")),
		})
		assert.Nil(t, dmnResourceDefinitions.JSON200)
		assert.NotNil(t, dmnResourceDefinitions.JSON400)
		assert.Equal(t, "BAD_REQUEST", dmnResourceDefinitions.JSON400.Code)
		assert.Equal(t,
			"unexpected GetDmnResourceDefinitionsRequest.SortBy: unsupportedSortColumn, supported: [key version dmnDefinitionName dmnResourceDefinitionId]",
			dmnResourceDefinitions.JSON400.Message,
		)
	})
	t.Run("GetDmnResourceDefinitions. Provide wrong sortOrder, expect Bad Request", func(t *testing.T) {
		dmnResourceDefinitions, _ := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			SortOrder: (*zenclient.GetDmnResourceDefinitionsParamsSortOrder)(new("unsupportedSortOrder")),
		})
		assert.Nil(t, dmnResourceDefinitions.JSON200)
		assert.NotNil(t, dmnResourceDefinitions.JSON400)
		assert.Equal(t, "BAD_REQUEST", dmnResourceDefinitions.JSON400.Code)
		assert.Equal(t,
			"unexpected GetDmnResourceDefinitionsRequest.SortOrder: unsupportedSortOrder, supported: [asc desc]",
			dmnResourceDefinitions.JSON400.Message,
		)
	})
}

func deployDmnResourceDefinition(t testing.TB, filename string) (*zenclient.CreateDmnResourceDefinitionResponse, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = strings.ReplaceAll(wd, filepath.Join("test", "e2e"), "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", filename)
	file, err := os.Open(loc)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	return app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(t.Context(), "application/xml", file)
}

func deployDmnResourceDefinitionE2e(t testing.TB, filePath string) (*zenclient.CreateDmnResourceDefinitionResponse, error) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	loc := filepath.Join(wd, filePath)

	file, err := os.Open(loc)
	require.NoError(t, err)

	return app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(t.Context(), "application/xml", file)
}

func deployDmnResourceDefinitionWithNewNameAndId(t testing.TB, filename string, newDmnDefinitionName, newDmnResourceDefinitionId *string) (int64, error) {
	response, err := deployDmnResourceDefinitionWithNewNameAndIdResponse(t, filename, newDmnDefinitionName, newDmnResourceDefinitionId)
	if err != nil {
		return 0, err
	}
	if response.JSON201 != nil {
		return response.JSON201.DmnResourceDefinitionKey, nil
	}
	if response.JSON200 != nil {
		return response.JSON200.DmnResourceDefinitionKey, nil
	}
	return 0, fmt.Errorf("failed to deploy dmn resource definition: %s returned status %d", filename, response.StatusCode())
}

func deployDmnResourceDefinitionWithNewNameAndIdResponse(t testing.TB, filename string, newDmnDefinitionName, newDmnResourceDefinitionId *string) (*zenclient.CreateDmnResourceDefinitionResponse, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	wd = strings.ReplaceAll(wd, filepath.Join("test", "e2e"), "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	stringFile := string(file)
	if newDmnDefinitionName != nil {
		stringFile = strings.ReplaceAll(stringFile, "name=\"DRD\"", "name=\""+*newDmnDefinitionName+"\"")
	}
	if newDmnResourceDefinitionId != nil {
		stringFile = strings.ReplaceAll(stringFile, "example_canAutoLiquidate", *newDmnResourceDefinitionId)
	}
	fileReader := strings.NewReader(stringFile)
	response, err := app.restClient.CreateDmnResourceDefinitionWithBodyWithResponse(t.Context(), "application/xml", fileReader)
	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return response, nil
		}
		return nil, fmt.Errorf("failed to deploy dmn resource definition: %s %w", filename, err)
	}
	return response, nil
}

func listDecisionDefinitions(t testing.TB, params *zenclient.GetDmnResourceDefinitionsParams) ([]zenclient.DmnResourceDefinitionSimple, error) {
	response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), params)
	if err != nil {
		return nil, fmt.Errorf("failed to list dmn resource definitions: %w", err)
	}
	return response.JSON200.Items, nil
}

func uniqueDmnResourceDefinitionTestValue(prefix string) string {
	return fmt.Sprintf("%s%d", prefix, time.Now().UnixNano())
}
