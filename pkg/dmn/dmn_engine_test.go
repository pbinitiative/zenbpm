package dmn_test

import (
	"errors"
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

func TestBulkEvaluation(t *testing.T) {
	// setup
	dmnEngine := dmn.New()

	bulkTestConfigs, err := loadBulkTestConfigs()

	if err != nil {
		t.Fatalf("Failed to load bulk evaluation test: %v", err)
	}

	for _, configuration := range bulkTestConfigs {
		dmnDefinition, _ := dmnEngine.LoadFromFile("./test-data/bulk-evaluation-test/" + configuration.DMN)

		for i, test := range configuration.Tests {
			result, err := dmnEngine.EvaluateDRD(dmnDefinition, test.Decision, test.Input)

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
		}
	}

}

func loadBulkTestConfigs() ([]configuration, error) {
	dir := "./test-data/bulk-evaluation-test"
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	configurations := make([]configuration, 0)

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".dmn" {
			dmnFile := file.Name()
			testFile := strings.TrimSuffix(dmnFile, ".dmn") + "_test.yml"

			configFilePath := filepath.Join(dir, testFile)
			_, err := os.Stat(configFilePath)

			if os.IsNotExist(err) {
				return nil, errors.New("Found DMN file without corresponding test file: [" + dmnFile + "]. Create test file: [" + testFile + "] to fix this issue")
			}

			data, err := os.ReadFile(filepath.Join(dir, testFile))
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
