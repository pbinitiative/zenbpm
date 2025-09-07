// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"context"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/storage"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

func (engine *Engine) createUserTask(ctx context.Context, batch storage.Batch, instance *runtime.ProcessInstance, element bpmn20.InternalTask, currentToken runtime.ExecutionToken) (runtime.ActivityState, error) {
	// TODO consider different handlers, since Service Tasks are different in their definition than user tasks
	return engine.createInternalTask(ctx, batch, instance, element, currentToken)
}
