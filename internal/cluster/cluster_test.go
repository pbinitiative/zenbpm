package cluster

import (
	"github.com/pbinitiative/zenbpm/pkg/storage"
	"testing"
)

func Test_ZenNode_implements_PersistentStorage(t *testing.T) {
	var _ storage.PersistentStorage = &ZenNode{}
}
