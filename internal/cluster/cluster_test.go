package cluster

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
)

func Test_ZenNode_implements_PersistentStorage(t *testing.T) {
	var _ storage.PersistentStorage = &ZenNode{}
}
