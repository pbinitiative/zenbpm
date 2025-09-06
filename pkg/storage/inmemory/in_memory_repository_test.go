// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

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
