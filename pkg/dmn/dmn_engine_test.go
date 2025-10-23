package dmn

import (
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
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
	dmnEngine.persistence = inmemory.NewStorage()
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	metadata, decisions, err := dmnEngine.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)

	assert.Equal(t, int64(1), metadata.Version)
	assert.Less(t, int64(1), metadata.Key)
	assert.Equal(t, "example_canAutoLiquidate", metadata.Id)
	assert.Equal(t, xmldata, metadata.DmnData)
	assert.Equal(t, md5.Sum(xmldata), metadata.DmnChecksum)

	assert.Equal(t, 1, len(decisions))
	assert.Equal(t, "example_canAutoLiquidateRule", decisions[0].Id)
	assert.Equal(t, int64(1), decisions[0].Version)
	assert.Equal(t, "example_canAutoLiquidate", decisions[0].DecisionDefinitionId)
	assert.Equal(t, metadata.Key, decisions[0].DmnResourceDefinitionKey)
	assert.Equal(t, "versionTagTest", decisions[0].VersionTag)
}

func TestLoadingTheSameFileWillNotIncreaseTheVersionNorChangeTheDecisionDefinitionKey(t *testing.T) {
	// setup
	dmnEngine.persistence = inmemory.NewStorage()
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)

	//run
	metadata, decisions, err := dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		"",
		definition,
		xmldata,
		dmnEngine.generateKey(),
	)
	assert.NoError(t, err)
	keyOne := metadata.Key
	assert.Equal(t, int64(1), metadata.Version)
	assert.Equal(t, 1, len(decisions))
	assert.Equal(t, int64(1), decisions[0].Version)

	//run 2
	metadata, decisions, err = dmnEngine.SaveDmnResourceDefinition(
		t.Context(),
		"",
		definition,
		xmldata,
		dmnEngine.generateKey(),
	)
	assert.NoError(t, err)
	keyTwo := metadata.Key
	assert.Equal(t, int64(1), metadata.Version)
	assert.Equal(t, 0, len(decisions))
	assert.Equal(t, keyTwo, keyOne)
}

func TestLoadingTheSameDecisionDefinitionWithModificationWillCreateNewVersion(t *testing.T) {
	// setup
	dmnEngine.persistence = inmemory.NewStorage()
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	definition2, xmldata2, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule-modified.dmn"))
	assert.NoError(t, err)

	decisionDefinition1, decisions1, err := dmnEngine.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)
	decisionDefinition2, decisions2, err := dmnEngine.SaveDmnResourceDefinition(t.Context(), "", definition2, xmldata2, dmnEngine.generateKey())
	assert.NoError(t, err)
	decisionDefinition3, decisions3, err := dmnEngine.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)

	assert.Equal(t, decisionDefinition1.Id, decisionDefinition2.Id, "both prepared files should have equal IDs")
	assert.Equal(t, int64(1), decisionDefinition1.Version)
	assert.Equal(t, int64(2), decisionDefinition2.Version)
	assert.Equal(t, int64(3), decisionDefinition3.Version)

	assert.Equal(t, 1, len(decisions1))
	assert.Equal(t, int64(1), decisions1[0].Version)
	assert.Equal(t, 1, len(decisions2))
	assert.Equal(t, int64(2), decisions2[0].Version)
	assert.Equal(t, 1, len(decisions3))
	assert.Equal(t, int64(3), decisions3[0].Version)

	assert.NotEqual(t, decisionDefinition2.Key, decisionDefinition1.Key)
}

func TestMultipleEnginesCreateUniqueIds(t *testing.T) {
	// setup
	dmnEngine.persistence = inmemory.NewStorage()
	store := inmemory.NewStorage()
	dmnEngine1 := NewEngine(EngineWithStorage(store))
	store2 := inmemory.NewStorage()
	dmnEngine2 := NewEngine(EngineWithStorage(store2))

	// when
	definition, xmldata, err := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", "can-autoliquidate-rule.dmn"))
	assert.NoError(t, err)
	decisionDefinition1, decisions1, err := dmnEngine1.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)
	decisionDefinition2, decisions2, err := dmnEngine2.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())
	assert.NoError(t, err)

	// then
	assert.NotEqual(t, decisionDefinition2.Key, decisionDefinition1.Key)
	assert.Equal(t, 1, len(decisions1))
	assert.Equal(t, 1, len(decisions2))
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

func TestBulkEvaluateDRD(t *testing.T) {
	// setup
	dmnEngine.persistence = inmemory.NewStorage()
	bulkTestConfigs, err := loadBulkTestConfigs()

	if err != nil {
		t.Fatalf("Failed to saveDecisionDefinition bulk evaluation test: %v", err)
	}

	for _, configuration := range bulkTestConfigs {
		definition, xmldata, loadErr := dmnEngine.ParseDmnFromFile(filepath.Join(".", "test-data", "bulk-evaluation-test", configuration.DMN))
		if loadErr != nil {
			t.Fatalf("Failed to saveDecisionDefinition DMN file - %v", loadErr.Error())
		}
		dmnDefinition, _, loadErr := dmnEngine.SaveDmnResourceDefinition(t.Context(), "", definition, xmldata, dmnEngine.generateKey())

		if loadErr != nil {
			t.Fatalf("Failed to saveDecisionDefinition DMN file - %v", loadErr.Error())
		}

		for i, test := range configuration.Tests {
			testName := fmt.Sprintf("decision: '%s', input: %v", test.Decision, test.Input)
			t.Run(testName, func(t *testing.T) {
				result, err := dmnEngine.FindAndEvaluateDRD(t.Context(), "latest", dmnDefinition.Id+"."+test.Decision, "", test.Input)

				if err != nil {
					t.Fatalf("Failed: %v", err)
					return
				}

				if !reflect.DeepEqual(test.ExpectedOutput, result.DecisionOutput) {
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
	files, err := os.ReadDir(filepath.Join(".", "test-data", "bulk-evaluation-test"))
	if err != nil {
		return nil, err
	}

	configurations := make([]configuration, 0)

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".dmn" {
			dmnFile := file.Name()
			testFile := strings.TrimSuffix(dmnFile, ".dmn") + ".results.yml"

			configFilePath := filepath.Join(".", "test-data", "bulk-evaluation-test", testFile)
			_, err := os.Stat(configFilePath)

			if os.IsNotExist(err) {
				return nil, errors.New("Found DMN file without corresponding test file: [" + dmnFile + "]. Create test file: [" + testFile + "] to fix this issue")
			}

			data, err := os.ReadFile(filepath.Join(".", "test-data", "bulk-evaluation-test", testFile))
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
