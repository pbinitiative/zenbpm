package inmemory_test

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/storage"
	"github.com/pbinitiative/zenbpm/pkg/storage/inmemory"
	"github.com/pbinitiative/zenbpm/pkg/storage/storagetest"
)

func TestInMemoryStorage(t *testing.T) {
	var store storage.Storage = inmemory.NewStorage()

	tester := storagetest.StorageTester{}

	tests := tester.GetTests()
	tester.PrepareTestData(store, t)
	for name, testFunc := range tests {
		t.Run(name, testFunc(store, t))
	}
}
