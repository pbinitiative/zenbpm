package runtime

import "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"

type DecisionDefinition struct {
	Version         int64            // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Key             int64            // The engines key for this given decision definition with version
	Id              string           // The ID as defined in the DMN file
	Definitions     dmn.TDefinitions // parsed file content
	DmnData         []byte           // the raw source data, compressed and encoded via ascii85
	DmnResourceName string           // some name for the resource
	DmnChecksum     [16]byte         // internal checksum to identify different versions
}

type Decision struct {
	Version               int64  // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Key                   int64  // The engines key for this given decision definition with version
	Id                    string // The decision ID as defined in the DMN file
	VersionTag            string // The VersionTag as defined in the DMN file
	DecisionDefinitionId  string // The DecisionDefinitionId is the parent DMN file's id
	DecisionDefinitionKey string // An id to decision definition that hosts the decision table
}
