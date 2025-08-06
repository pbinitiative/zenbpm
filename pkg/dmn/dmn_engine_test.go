package dmn

import (
	"errors"
	"fmt"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var dmnEngine *ZenDmnEngine
var engineStorage *inmemory.Storage

func TestMain(m *testing.M) {
	// setup
	engineStorage = inmemory.NewStorage()

	var exitCode int

	defer func() {
		os.Exit(exitCode)
	}()

	dmnEngine = NewEngine(EngineWithStorage(engineStorage))

	// Run the tests
	exitCode = m.Run()
}

func TestMetadataIsGivenFromLoadedXmlFile(t *testing.T) {
	// setup
	metadata, decisions, _ := dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")

	assert.Equal(t, int64(1), metadata.Version)
	assert.Greater(t, metadata.Key, int64(1))
	assert.Equal(t, "example_canAutoLiquidate", metadata.Id)
}

func TestLoadingTheSameFileWillNotIncreaseTheVersionNorChangeTheDecisionDefinitionKey(t *testing.T) {
	// setup and test
	metadata, decisions, _ := dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")
	keyOne := metadata.Key
	assert.Equal(t, int64(1), metadata.Version)

	metadata, decisions, _ = dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")
	keyTwo := metadata.Key
	assert.Equal(t, int64(1), metadata.Version)
	assert.Equal(t, keyTwo, keyOne)
}

func TestLoadingTheSameDecisionDefinitionWithModificationWillCreateNewVersion(t *testing.T) {
	// setup
	decisionDefinition1, decisions, _ := dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")
	decisionDefinition2, decisions, _ := dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule-modified.dmn")
	decisionDefinition3, decisions, _ := dmnEngine.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")

	assert.Equal(t, decisionDefinition1.Id, decisionDefinition2.Id, "both prepared files should have equal IDs")
	assert.Equal(t, int64(1), decisionDefinition1.Version)
	assert.Equal(t, int64(2), decisionDefinition2.Version)
	assert.Equal(t, int64(3), decisionDefinition3.Version)

	assert.NotEqual(t, decisionDefinition2.Key, decisionDefinition1.Key)
}

func TestMultipleEnginesCreateUniqueIds(t *testing.T) {
	// setup
	store := inmemory.NewStorage()
	dmnEngine1 := NewEngine(EngineWithStorage(store))
	store2 := inmemory.NewStorage()
	dmnEngine2 := NewEngine(EngineWithStorage(store2))

	// when
	decisionDefinition1, decisions, err := dmnEngine1.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")
	assert.NoError(t, err)
	decisionDefinition2, decisions, err := dmnEngine2.LoadFromFile(t.Context(), "./test-data/bulk-evaluation-test/can-autoliquidate-rule.dmn")
	assert.NoError(t, err)

	// then
	assert.NotEqual(t, decisionDefinition2.Key, decisionDefinition1.Key)
}

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
	bulkTestConfigs, err := loadBulkTestConfigs()

	if err != nil {
		t.Fatalf("Failed to load bulk evaluation test: %v", err)
	}

	for _, configuration := range bulkTestConfigs {
		dmnDefinition, decisions, loadErr := dmnEngine.LoadFromFile(t.Context(), BulkEvaluationTestPath+"/"+configuration.DMN)

		if loadErr != nil {
			t.Fatalf("Failed to load DMN file - %v", loadErr.Error())
		}

		for i, test := range configuration.Tests {
			testName := fmt.Sprintf("decision: '%s', input: %v", test.Decision, test.Input)
			t.Run(testName, func(t *testing.T) {
				result, err := dmnEngine.EvaluateDRD(t.Context(), dmnDefinition, test.Decision, test.Input)

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
