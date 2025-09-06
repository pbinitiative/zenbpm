// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	stdruntime "runtime"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
)

type StorageTestFunc func(s Storage, t *testing.T) func(t *testing.T)

func GetTests() map[string]StorageTestFunc {
	tests := map[string]StorageTestFunc{}

	functions := []StorageTestFunc{
		TestSaveProcessDefinition,
	}

	for _, function := range functions {
		tests[getFunctionName(function)] = function
	}
	return tests
}

func getFunctionName(i any) string {
	return stdruntime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func TestSaveProcessDefinition(s Storage, t *testing.T) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.TODO()

		r := rand.Int63()

		def := runtime.ProcessDefinition{
			BpmnProcessId:    fmt.Sprintf("id-%d", r),
			Version:          1,
			Key:              r,
			BpmnData:         fmt.Sprintf("data-%d", r),
			BpmnChecksum:     [16]byte{1},
			BpmnResourceName: fmt.Sprintf("resource-%d", r),
		}

		err := s.SaveProcessDefinition(ctx, def)
		assert.Nil(t, err)

		definition, err := s.FindProcessDefinitionByKey(ctx, r)
		assert.Nil(t, err)
		assert.Equal(t, r, definition.Key, "Process key is as expected")
	}
}
