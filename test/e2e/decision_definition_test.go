package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	"github.com/pbinitiative/zenbpm/pkg/zenclient"
	"github.com/stretchr/testify/assert"
)

func TestRestApiDmnResourceDefinition(t *testing.T) {
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
		assert.NoError(t, err)
	})

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		err := deployDmnResourceDefinition(t, "can-autoliquidate-rule.dmn")
		assert.NoError(t, err)
		definitions, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		count := 0
		for _, def := range definitions {
			if *def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
				count++
			}
		}
		assert.Equal(t, 1, count)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition public.DmnResourceDefinitionSimple
		for _, def := range list {
			if *def.DmnResourceDefinitionId == "example_canAutoLiquidate" {
				deployedDefinition = def
				break
			}
		}
		assert.Equal(t, "example_canAutoLiquidate", *deployedDefinition.DmnResourceDefinitionId)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)

		detail, err := getDmnResourceDefinitionDetail(t, list[0].Key)
		assert.NoError(t, err)
		assert.Equal(t, "example_canAutoLiquidate", *detail.DmnResourceDefinitionId)
		assert.NotNil(t, detail.DmnData)
	})
}

func TestGetDmnResourceDefinitions(t *testing.T) {
	t.Run("deploy dmn resource definition", func(t *testing.T) {
		err := deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("name11"), ptr.To("defId1"))
		assert.NoError(t, err)
		err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("name12"), ptr.To("defId1"))
		assert.NoError(t, err)
		err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("name21"), ptr.To("defId2"))
		assert.NoError(t, err)
		err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("name31"), ptr.To("defId3"))
		assert.NoError(t, err)
		err = deployDmnResourceDefinitionWithNewNameAndId(t, "can-autoliquidate-rule.dmn", ptr.To("jmeno41"), ptr.To("defId4"))
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
		assert.Equal(t, "defId1", *processInstances.JSON200.Items[0].DmnResourceDefinitionId)
		assert.Equal(t, "name12", processInstances.JSON200.Items[1].DmnDefinitionName)
		assert.Equal(t, "defId1", *processInstances.JSON200.Items[1].DmnResourceDefinitionId)
	})
}

func getDmnResourceDefinitionDetail(t testing.TB, key int64) (public.DmnResourceDefinitionDetail, error) {
	var detail public.DmnResourceDefinitionDetail
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/dmn-resource-definitions/%d", key)).
		DoOk()
	if err != nil {
		return detail, fmt.Errorf("failed to get %d dmn resource definition detail: %w", key, err)
	}
	err = json.Unmarshal(resp, &detail)
	if err != nil {
		return detail, fmt.Errorf("failed to unmarshal %d dmn resource definition detail: %w", key, err)
	}
	return detail, nil
}

func deployDmnResourceDefinition(t testing.TB, filename string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	wd = strings.ReplaceAll(wd, filepath.Join("test", "e2e"), "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", "bulk-evaluation-test", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/dmn-resource-definitions").
		WithMethod("POST").
		WithBody(file).
		WithHeader("Content-Type", "application/xml").
		DoOk()
	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return nil
		}
		return fmt.Errorf("failed to deploy dmn resource definition: %s %w", string(resp), err)
	}
	definition := public.CreateDmnResourceDefinition201JSONResponse{}
	err = json.Unmarshal(resp, &definition)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}

func deployDmnResourceDefinitionWithNewNameAndId(t testing.TB, filename string, newDmnDefinitionName, newDmnResourceDefinitionId *string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	wd = strings.ReplaceAll(wd, filepath.Join("test", "e2e"), "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", "bulk-evaluation-test", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	stringFile := string(file)
	if newDmnDefinitionName != nil {
		stringFile = strings.ReplaceAll(stringFile, "name=\"DRD\"", "name=\""+*newDmnDefinitionName+"\"")
	}
	if newDmnResourceDefinitionId != nil {
		stringFile = strings.ReplaceAll(stringFile, "example_canAutoLiquidate", *newDmnResourceDefinitionId)
	}
	file = []byte(stringFile)
	resp, err := app.NewRequest(t).
		WithPath("/v1/dmn-resource-definitions").
		WithMethod("POST").
		WithBody(file).
		WithHeader("Content-Type", "application/xml").
		DoOk()
	if err != nil {
		if strings.Contains(err.Error(), "DUPLICATE") {
			return nil
		}
		return fmt.Errorf("failed to deploy dmn resource definition: %s %w", string(resp), err)
	}
	definition := public.CreateDmnResourceDefinition201JSONResponse{}
	err = json.Unmarshal(resp, &definition)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}

func listDecisionDefinitions(t testing.TB) ([]public.DmnResourceDefinitionSimple, error) {
	respBytes, err := app.NewRequest(t).
		WithPath("/v1/dmn-resource-definitions").
		WithMethod("GET").
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to list dmn resource definitions: %w", err)
	}
	resp := public.GetDmnResourceDefinitions200JSONResponse{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal dmn resource definitions: %w", err)
	}
	return resp.Items, nil
}
