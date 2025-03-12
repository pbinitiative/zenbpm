package dmn_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/dmn"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

type testCase struct {
	Decision       string                 `yaml:"decision"`
	Input          map[string]interface{} `yaml:"input"`
	ExpectedOutput interface{}            `yaml:"expectedOutput"`
}

type configuration struct {
	DMN   string
	Tests []testCase `yaml:"tests"`
}

const BulkEvaluationTestPath = "./test-data/bulk-evaluation-test"

func Test_BulkEvaluateDRD(t *testing.T) {
	// setup
	dmnEngine := dmn.New()
	ctx := context.Background()

	bulkTestConfigs, err := loadBulkTestConfigs()

	if err != nil {
		t.Fatalf("Failed to load bulk evaluation test: %v", err)
	}

	for _, configuration := range bulkTestConfigs {
		dmnDefinition, loadErr := dmnEngine.LoadFromFile(ctx, BulkEvaluationTestPath+"/"+configuration.DMN)

		if loadErr != nil {
			t.Fatalf("Failed to load DMN file - %v", loadErr.Error())
		}

		for i, test := range configuration.Tests {
			testName := fmt.Sprintf("decision: '%s', input: %v", test.Decision, test.Input)
			t.Run(testName, func(t *testing.T) {
				result, err := dmnEngine.EvaluateDRD(ctx, dmnDefinition, test.Decision, test.Input)

				if err != nil {
					t.Fatalf("Failed: %v", err)
					return
				}

				if !reflect.DeepEqual(result.DecisionOutput, test.ExpectedOutput) {
					t.Errorf("\n"+
						"Test:      %v\n"+
						"Decision:  %v\n"+
						"Index:     %v\n"+
						"Inputs:    %v\n"+
						"Expected:  %v\n"+
						"Got:       %v",
						configuration.DMN,
						test.Decision,
						strconv.Itoa(i),
						test.Input,
						test.ExpectedOutput,
						result.DecisionOutput,
					)
				}
			})
		}
	}

}

func loadBulkTestConfigs() ([]configuration, error) {
	files, err := os.ReadDir(BulkEvaluationTestPath)
	if err != nil {
		return nil, err
	}

	configurations := make([]configuration, 0)

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".dmn" {
			dmnFile := file.Name()
			testFile := strings.TrimSuffix(dmnFile, ".dmn") + ".results.yml"

			configFilePath := filepath.Join(BulkEvaluationTestPath, testFile)
			_, err := os.Stat(configFilePath)

			if os.IsNotExist(err) {
				return nil, errors.New("Found DMN file without corresponding test file: [" + dmnFile + "]. Create test file: [" + testFile + "] to fix this issue")
			}

			data, err := os.ReadFile(filepath.Join(BulkEvaluationTestPath, testFile))
			if err != nil {
				return nil, err
			}

			var config configuration
			err = yaml.Unmarshal(data, &config)
			if err != nil {
				return nil, err
			}

			config.DMN = dmnFile
			configurations = append(configurations, config)
		}
	}

	return configurations, nil
}
