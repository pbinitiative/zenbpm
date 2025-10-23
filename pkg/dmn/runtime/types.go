package runtime

import (
	"time"

	"github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
)

type DmnResourceDefinition struct {
	Version         int64            // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Key             int64            // The engines key for this given dmn resource definition with version
	Id              string           // The ID as defined in the DMN file
	Definitions     dmn.TDefinitions // parsed file content
	DmnData         []byte           // the raw source data, compressed and encoded via ascii85
	DmnResourceName string           // some name for the resource
	DmnChecksum     [16]byte         // internal checksum to identify different versions
}

type DecisionDefinition struct {
	Version                  int64  // A version of the process, default=1, incremented, when another process with the same ID is loaded
	Id                       string // The decision ID as defined in the DMN file
	VersionTag               string // The VersionTag as defined in the DMN file
	DecisionDefinitionId     string // The DecisionDefinitionId is the parent DMN file's id
	DmnResourceDefinitionKey int64  // An id to dmn resource definition that hosts the decision table
}
type DecisionInstance struct {
	Key                int64     // int64 id of the decision result
	ElementInstanceKey int64     // int64 id of the element instance
	ElementId          string    // id of the element instance
	ProcessInstanceKey int64     // int64 reference to process instance
	ExecutionTokenKey  int64     // key of the execution token that executed the decision
	DecisionId         string    // id of the decision from xml
	CreatedAt          time.Time // time of creation
	OutputVariables    string    // serialized json output variables
	EvaluatedDecisions string    // serialized json evaluated decisions
}
