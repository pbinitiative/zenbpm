// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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

func TestRestApiDecisionDefinition(t *testing.T) {
	t.Run("deploy decision definition", func(t *testing.T) {
		err := deployDecisionDefinition(t, "can-autoliquidate-rule.dmn")
		assert.NoError(t, err)
	})

	t.Run("repeatedly calling rest api to deploy definition", func(t *testing.T) {
		err := deployDecisionDefinition(t, "can-autoliquidate-rule.dmn")
		assert.NoError(t, err)
		defintitions, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		count := 0
		for _, def := range defintitions {
			if def.DecisionDefinitionId == "example_canAutoLiquidate" {
				count++
			}
		}
		assert.Equal(t, 1, count)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)
		var deployedDefinition public.DecisionDefinitionSimple
		for _, def := range list {
			if def.DecisionDefinitionId == "example_canAutoLiquidate" {
				deployedDefinition = def
				break
			}
		}
		assert.Equal(t, "example_canAutoLiquidate", deployedDefinition.DecisionDefinitionId)
	})

	t.Run("listing deployed definitions", func(t *testing.T) {
		list, err := listDecisionDefinitions(t)
		assert.NoError(t, err)
		assert.Greater(t, len(list), 0)

		detail, err := getDecisionDefinitionDetail(t, list[0].Key)
		assert.NoError(t, err)
		assert.Equal(t, "example_canAutoLiquidate", detail.DecisionDefinitionId)
		assert.NotNil(t, detail.DmnData)
	})
}

func getDecisionDefinitionDetail(t testing.TB, key string) (public.DecisionDefinitionDetail, error) {
	var detail public.DecisionDefinitionDetail
	resp, err := app.NewRequest(t).
		WithPath(fmt.Sprintf("/v1/decision-definitions/%s", key)).
		DoOk()
	if err != nil {
		return detail, fmt.Errorf("failed to get %s decision definition detail: %w", key, err)
	}
	err = json.Unmarshal(resp, &detail)
	if err != nil {
		return detail, fmt.Errorf("failed to unmarshal %s decision definition detail: %w", key, err)
	}
	return detail, nil
}

func deployDecisionDefinition(t testing.TB, filename string) error {
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	wd = strings.ReplaceAll(wd, "/test/e2e", "")
	loc := filepath.Join(wd, "pkg", "dmn", "test-data", "bulk-evaluation-test", filename)
	file, err := os.ReadFile(loc)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	resp, err := app.NewRequest(t).
		WithPath("/v1/decision-definitions").
		WithMethod("POST").
		WithBody(file).
		WithHeader("Content-Type", "application/xml").
		DoOk()
	if err != nil {
		return fmt.Errorf("failed to deploy decision definition: %s %w", string(resp), err)
	}
	definition := public.CreateDecisionDefinition200JSONResponse{}
	err = json.Unmarshal(resp, &definition)
	if err != nil {
		return fmt.Errorf("failed to unmarshal create definition response: %w", err)
	}
	return nil
}

func listDecisionDefinitions(t testing.TB) ([]public.DecisionDefinitionSimple, error) {
	respBytes, err := app.NewRequest(t).
		WithPath("/v1/decision-definitions").
		WithMethod("GET").
		DoOk()
	if err != nil {
		return nil, fmt.Errorf("failed to list decision definitions: %w", err)
	}
	resp := public.GetDecisionDefinitions200JSONResponse{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal decision definitions: %w", err)
	}
	return resp.Items, nil
}
