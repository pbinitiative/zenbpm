package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiDmnResourceDefinition(t *testing.T) {
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		response, err := deployDmnResourceDefinition(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn")
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
		response, err := deployDmnResourceDefinition(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn")
		assert.Nil(t, response.JSON201)
		assert.NotNil(t, response.JSON409)
		assert.Equal(t, "CONFLICT", response.JSON409.Code)
		assert.Contains(t, response.JSON409.Message, "duplicate dmn resource definition")

		definitions, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{})
		assert.NoError(t, err)
		count := 0
		for _, def := range definitions {
			if def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
				count++
			}
		}
		assert.Equal(t, 1, count)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{})
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition zenclient.DmnResourceDefinitionSimple
		for _, def := range list {
			if def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
				deployedDefinition = def
				break
			}
		}
		assert.Equal(t, "example_canAutoLiquidate", deployedDefinition.DmnResourceDefinitionId)
	})

	t.Run("get single deployed definition", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("singleDeployment1"), ptr.To("deploymentDefId1"))
		assert.NoError(t, err)

		list, err := listDecisionDefinitions(t, &zenclient.GetDmnResourceDefinitionsParams{
			DmnDefinitionName: ptr.To("singleDeployment1"),
		})
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)

		var found *zenclient.DmnResourceDefinitionSimple

		for i := range list {
			if list[i].DmnDefinitionName == "singleDeployment1" &&
				list[i].DmnResourceDefinitionId == "deploymentDefId1" {
				found = &list[i]
				break
			}
		}

		detail, err := app.restClient.GetDmnResourceDefinitionWithResponse(t.Context(), found.Key)
		assert.NoError(t, err)
		assert.Equal(t, found.DmnResourceDefinitionId, detail.JSON200.DmnResourceDefinitionId)
		assert.Equal(t, found.DmnDefinitionName, detail.JSON200.DmnDefinitionName)
		assert.NotNil(t, detail.JSON200.DmnData)
	})
}

func TestGetDmnResourceDefinitionNotFound(t *testing.T) {
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
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		_, err := deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("name11"), ptr.To("defId1"))
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("name12"), ptr.To("defId1"))
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("name21"), ptr.To("defId2"))
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("name31"), ptr.To("defId3"))
		assert.NoError(t, err)
		_, err = deployDmnResourceDefinitionWithNewNameAndId(t, "bulk-evaluation-test/can-autoliquidate-rule.dmn", ptr.To("jmeno41"), ptr.To("defId4"))
		assert.NoError(t, err)
	})

	t.Run("find dmn resource definition by name sub string case insensitive, onlyLatest=true, order by name desc", func(t *testing.T) {
		processInstances, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			DmnDefinitionName: ptr.To("AmE"),
			OnlyLatest:        ptr.To(true),
			SortBy:            ptr.To(zenclient.GetDmnResourceDefinitionsParamsSortByDmnDefinitionName),
			SortOrder:         ptr.To(zenclient.GetDmnResourceDefinitionsParamsSortOrderDesc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, processInstances.JSON200.TotalCount)
		assert.Equal(t, "name31", processInstances.JSON200.Items[0].DmnDefinitionName)
		assert.Equal(t, "name21", processInstances.JSON200.Items[1].DmnDefinitionName)
		assert.Equal(t, "name12", processInstances.JSON200.Items[2].DmnDefinitionName)
	})

	t.Run("find dmn resource definition by dmnResourceDefinitionId, order by name asc", func(t *testing.T) {
		processInstances, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			DmnResourceDefinitionId: ptr.To("defId1"),
			SortBy:                  ptr.To(zenclient.GetDmnResourceDefinitionsParamsSortByDmnDefinitionName),
			SortOrder:               ptr.To(zenclient.GetDmnResourceDefinitionsParamsSortOrderAsc),
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, processInstances.JSON200.TotalCount)
		assert.Equal(t, "name11", processInstances.JSON200.Items[0].DmnDefinitionName)
		assert.Equal(t, "defId1", processInstances.JSON200.Items[0].DmnResourceDefinitionId)
		assert.Equal(t, "name12", processInstances.JSON200.Items[1].DmnDefinitionName)
		assert.Equal(t, "defId1", processInstances.JSON200.Items[1].DmnResourceDefinitionId)
	})
}

func TestGetDmnResourceDefinitionsBadRequests(t *testing.T) {
	t.Run("GetDmnResourceDefinitions. Provide wrong sortBy, expect Bad Request", func(t *testing.T) {
		dmnResourceDefinitions, _ := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), &zenclient.GetDmnResourceDefinitionsParams{
			SortBy: (*zenclient.GetDmnResourceDefinitionsParamsSortBy)(ptr.To("unsupportedSortColumn")),
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
			SortOrder: (*zenclient.GetDmnResourceDefinitionsParamsSortOrder)(ptr.To("unsupportedSortOrder")),
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

func deployDmnResourceDefinitionWithNewNameAndId(t testing.TB, filename string, newDmnDefinitionName, newDmnResourceDefinitionId *string) (int64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	wd = strings.ReplaceAll(wd, filepath.Join("test", "e2e"), "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return 0, fmt.Errorf("failed to read file: %w", err)
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
			return 0, nil
		}
		return 0, fmt.Errorf("failed to deploy dmn resource definition: %s %w", filename, err)
	}
	return response.JSON201.DmnResourceDefinitionKey, nil
}

func listDecisionDefinitions(t testing.TB, params *zenclient.GetDmnResourceDefinitionsParams) ([]zenclient.DmnResourceDefinitionSimple, error) {
	response, err := app.restClient.GetDmnResourceDefinitionsWithResponse(t.Context(), params)
	if err != nil {
		return nil, fmt.Errorf("failed to list dmn resource definitions: %w", err)
	}
	return response.JSON200.Items, nil
}
