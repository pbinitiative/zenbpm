package storage_inmemory

import (
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
)

func Test_implements_ProcessDefinition_interface(t *testing.T) {
	var _ storage.ProcessDefinition = &processDefinition{}
}
