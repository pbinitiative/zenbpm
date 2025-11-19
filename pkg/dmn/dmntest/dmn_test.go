//go:build dmntest
// +build dmntest

package dmntest

import (
	"archive/zip"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/pbinitiative/feel"
	"github.com/pbinitiative/zenbpm/pkg/dmn"
	dmnModel "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/stretchr/testify/assert"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

var dmnEngine *dmn.ZenDmnEngine
var engineStorage *inmemory.Storage
var tmpDir string

type TestData struct {
	Labels    Labels     `xml:"labels"`
	TestCases []TestCase `xml:"testCase"`
}

type Labels struct {
	Label []string `xml:"label"`
}

type TestCase struct {
	Id          string       `xml:"id,attr"`
	Description string       `xml:"description"`
	InputNode   []InputNode  `xml:"inputNode"`
	ResultNode  []ResultNode `xml:"resultNode"`
}

type ResultNode struct {
	Name     string `xml:"name,attr"`
	Type     string `xml:"type,attr"`
	Expected Data   `xml:"expected"`
}

type InputNode struct {
	Name  string `xml:"name,attr"`
	Type  string `xml:"type,attr"`
	Input Data
}

func (inputNode *InputNode) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		Name      string      `xml:"name,attr"`
		Value     Value       `xml:"value"`
		Component []Component `xml:"component"`
		List      List        `xml:"list"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	inputNode.Input.Value = tempStruct.Value
	inputNode.Input.List.Items = tempStruct.List.Items
	inputNode.Input.Component = tempStruct.Component
	inputNode.Name = tempStruct.Name
	return nil
}

type Data struct {
	Value     Value       `xml:"value"`
	Component []Component `xml:"component"`
	List      List        `xml:"list"`
}

type List struct {
	Items []Data `xml:"item"`
}

type Component struct {
	Name string `xml:"name,attr"`
	Data Data
}

func (component *Component) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	tempStruct := struct {
		Name      string      `xml:"name,attr"`
		Value     Value       `xml:"value"`
		Component []Component `xml:"component"`
		List      List        `xml:"list"`
	}{}
	err := d.DecodeElement(&tempStruct, &start)
	if err != nil {
		return err
	}
	component.Data.Value = tempStruct.Value
	component.Data.List.Items = tempStruct.List.Items
	component.Data.Component = tempStruct.Component
	component.Name = tempStruct.Name
	return nil
}

type Value struct {
	Nil   string `xml:"nil,attr"`
	Type  string `xml:"type,attr"`
	Value string `xml:",chardata"`
}

func TestMain(m *testing.M) {
	// setup
	engineStorage = inmemory.NewStorage()

	var exitCode int

	defer func() {
		os.Exit(exitCode)
	}()

	tmpDir = os.TempDir()
	getTestData()

	dmnEngine = dmn.NewEngine(dmn.EngineWithStorage(engineStorage))

	// Run the tests
	exitCode = m.Run()
}

func TestDmnComplianceLevel2(t *testing.T) {
	root, err := getRootTestDir("2")
	assert.NoError(t, err)

	entries, err := os.ReadDir(root)
	assert.NoError(t, err)

	for _, testEntry := range entries {
		if !testEntry.IsDir() {
			continue
		}

		testData, definition, xmlData, err := loadTestFiles(root, testEntry)
		if err != nil {
			panic(err)
		}

		_, _, err = dmnEngine.SaveDecisionDefinition(t.Context(), "", definition, xmlData, engineStorage.GenerateId())
		assert.NoError(t, err)

		//map between decision Ids and Names
		decisionIdMap := make(map[string]string)
		for i := range definition.Decisions {
			decisionIdMap[definition.Decisions[i].Name] = definition.Decisions[i].Id
		}

		for _, testCase := range testData.TestCases {

			/* prepare input data for the tests*/
			inputData := make(map[string]interface{})
			for _, inputNode := range testCase.InputNode {
				inputData[inputNode.Name] = castTestData(inputNode.Input)
			}

			for _, testNode := range testCase.ResultNode {
				runTest(t, testEntry.Name()+"_"+testCase.Id+"_"+testNode.Name, decisionIdMap[testNode.Name], inputData, castTestData(testNode.Expected))
			}
		}
	}
}

func TestDmnComplianceLevel3(t *testing.T) {
	root, err := getRootTestDir("3")
	assert.NoError(t, err)

	entries, err := os.ReadDir(root)
	assert.NoError(t, err)

	for _, testEntry := range entries {
		if !testEntry.IsDir() {
			continue
		}

		testData, definition, xmlData, err := loadTestFiles(root, testEntry)
		if err != nil {
			panic(err)
		}

		_, _, err = dmnEngine.SaveDecisionDefinition(t.Context(), "", definition, xmlData, engineStorage.GenerateId())
		assert.NoError(t, err)

		//map between decision Ids and Names
		decisionIdMap := make(map[string]string)
		for i := range definition.Decisions {
			decisionIdMap[definition.Decisions[i].Name] = definition.Decisions[i].Id
		}

		for _, testCase := range testData.TestCases {

			/* prepare input data for the tests*/
			inputData := make(map[string]interface{})
			for _, inputNode := range testCase.InputNode {
				inputData[inputNode.Name] = castTestData(inputNode.Input)
			}

			for _, testNode := range testCase.ResultNode {
				runTest(t, testEntry.Name()+"_"+testCase.Id+"_"+testNode.Name, decisionIdMap[testNode.Name], inputData, castTestData(testNode.Expected))
			}
		}
	}
}

func castTestData(data Data) interface{} {
	if data.Value.Nil == "true" {
		return nil
	}
	inputValue := data.Value.Value
	inputType, _ := strings.CutPrefix(data.Value.Type, "xsd:")
	switch inputType {
	case "string":
		return inputValue
	case "":
		if data.Component != nil {
			result := make(map[string]interface{})
			for _, component := range data.Component {
				result[component.Name] = castTestData(component.Data)
			}
			return result
		} else if data.List.Items != nil {
			result := make([]interface{}, 0, len(data.List.Items))
			for _, item := range data.List.Items {
				result = append(result, castTestData(item))
			}
			return result
		} else if data.List.Items == nil {
			return make([]interface{}, 0)
		}
		panic("unsupported data type")
	default:
		result, err := feel.EvalString(inputValue)
		if err != nil {
			return inputValue
		}
		return result
	}
	return nil
}

func loadTestFiles(root string, testEntry os.DirEntry) (TestData, *dmnModel.TDefinitions, []byte, error) {
	testFiles, err := os.ReadDir(filepath.Join(root, testEntry.Name()))
	if err != nil {
		panic(err)
	}

	var testData TestData
	var definition *dmnModel.TDefinitions
	var xmlData []byte
	for _, file := range testFiles {
		if strings.HasSuffix(file.Name(), ".dmn") {
			definition, xmlData, err = dmnEngine.ParseDmnFromFile(filepath.Join(root, testEntry.Name(), file.Name()))
			if err != nil {
				panic(err)
			}
		} else if strings.HasSuffix(file.Name(), ".xml") {
			testXmlData, err := os.ReadFile(filepath.Join(root, testEntry.Name(), file.Name()))
			err = xml.Unmarshal(testXmlData, &testData)
			if err != nil {
				panic(err)
			}
		}
	}

	return testData, definition, xmlData, err
}

func runTest(t *testing.T, testName string, decisionId string, inputData map[string]interface{}, expectedResult any) {
	t.Run(testName, func(t *testing.T) {
		var err error
		defer func() {
			if r := recover(); r != nil {
				t.Error("Recovered from panic in evaluation:", r)
				fmt.Println("Stack trace:")
				fmt.Printf("%s\n", debug.Stack())
			}
		}()
		result, err := dmnEngine.FindAndEvaluateDRD(t.Context(), "latest", decisionId, "", inputData)
		assert.NoError(t, err)

		defer func() {
			if r := recover(); r != nil {
				t.Error("panic because resp was nil")
			}
		}()

		exp, err := json.Marshal(expectedResult)
		assert.NoError(t, err)
		out, err := json.Marshal(result.DecisionOutput)
		assert.NoError(t, err)
		assert.Equal(t, exp, out)
	})
}

func getTestData() {
	url := "https://api.github.com/repos/dmn-tck/tck/zipball/"

	zipFile := filepath.Join(tmpDir, "tck.zip")
	outDir := filepath.Join(tmpDir, "tck")

	if fileExists(filepath.Join(outDir, "success")) {
		return
	} else {
		os.Remove(zipFile)
		os.Remove(outDir)
	}

	// Download ZIP
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	out, err := os.Create(zipFile)
	if err != nil {
		panic(err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		panic(err)
	}

	// Unzip
	err = unzip(zipFile, outDir)
	if err != nil {
		panic(err)
	}

	os.Remove(zipFile)
	//save this file to know everything went well, now we don't have to download anymore
	f, _ := os.Create(filepath.Join(outDir, "success"))
	f.Close()
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	os.MkdirAll(dest, 0755)

	for _, f := range r.File {
		fpath := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(fpath, f.Mode())
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fpath), 0755); err != nil {
			return err
		}

		dstFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			dstFile.Close()
			return err
		}

		_, err = io.Copy(dstFile, rc)

		dstFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return false
}

func getRootTestDir(complianceLevel string) (string, error) {
	root := filepath.Join(tmpDir, "tck")
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}

	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(root, e.Name(), "TestCases", "compliance-level-"+complianceLevel), nil
		}
	}

	return "", fs.ErrNotExist
}
