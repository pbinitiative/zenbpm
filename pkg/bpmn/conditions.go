// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"fmt"
	"strings"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/model/bpmn20"
)

// exclusivelyFilterByConditionExpression
// [From BPMN 2.0 Specification, chapter 10.5.3 Inclusive Gateway]
// A diverging Exclusive Gateway (Decision) is used to create alternative paths within a Process flow. This is basically
// the “diversion point in the road” for a Process. For a given instance of the Process, only one of the paths can be taken.
// A Decision can be thought of as a question that is asked at a particular point in the Process. The question has a defined
// set of alternative answers. Each answer is associated with a condition Expression that is associated with a Gateway’s
// outgoing Sequence Flows.
// A default path can optionally be identified, to be taken in the event that none of the conditional Expressions evaluate
// to true. If a default path is not specified and the Process is executed such that none of the conditional Expressions
// evaluates to true, a runtime exception occurs.
// A converging Exclusive Gateway is used to merge alternative paths. Each incoming Sequence Flow token is routed
// to the outgoing Sequence Flow without synchronization.
func exclusivelyFilterByConditionExpression(flows []bpmn20.SequenceFlow, defaultFlow bpmn20.SequenceFlow, variableContext map[string]interface{}) ([]bpmn20.SequenceFlow, error) {
	var ret []bpmn20.SequenceFlow
	flowIds := strings.Builder{}
	for _, flow := range flows {
		expression := flow.GetConditionExpression()
		if expression != "" {
			flowIds.WriteString(fmt.Sprintf("[id='%s',name='%s']", flow.GetId(), flow.GetName()))
			out, err := evaluateExpression(expression, variableContext)
			if err != nil {
				return nil, &ExpressionEvaluationError{
					Msg: fmt.Sprintf("Error evaluating expression in flow element id='%s' name='%s'", flow.GetId(), flow.GetName()),
					Err: err,
				}
			}
			if out == true {
				ret = append(ret, flow)
				break
			}
			// one unconditional flow is enough to proceed further
		} else if len(flows) == 1 {
			ret = append(ret, flow)
		}
	}
	if len(ret) == 0 {
		if defaultFlow == nil {
			return nil, &ExpressionEvaluationError{
				Msg: fmt.Sprintf("No default flow, nor matching expressions found, for flow elements: %s", flowIds.String()),
				Err: nil,
			}
		}
		ret = append(ret, defaultFlow)
	}
	return ret, nil
}

// inclusivelyFilterByConditionExpression
// [From BPMN 2.0 Specification, chapter 10.5.3 Inclusive Gateway]
// A diverging Inclusive Gateway (Inclusive Decision) can be used to create alternative but also parallel paths within a
// Process flow. Unlike the Exclusive Gateway, all condition Expressions are evaluated. The true evaluation of one
// condition Expression does not exclude the evaluation of other condition Expressions. All Sequence Flows with
// a true evaluation will be traversed by a token. Since each path is considered to be independent, all combinations of the
// paths MAY be taken, from zero to all.
func inclusivelyFilterByConditionExpression(flows []bpmn20.SequenceFlow, defaultFlow bpmn20.SequenceFlow, variableContext map[string]interface{}) ([]bpmn20.SequenceFlow, error) {
	var ret []bpmn20.SequenceFlow
	for _, flow := range flows {
		expression := flow.GetConditionExpression()
		if expression != "" {
			out, err := evaluateExpression(expression, variableContext)
			if err != nil {
				return nil, &ExpressionEvaluationError{
					Msg: fmt.Sprintf("Error evaluating expression in flow element id='%s' name='%s'", flow.GetId(), flow.GetName()),
					Err: err,
				}
			}
			if out == true {
				ret = append(ret, flow)
			}
			// if there is one outgoing flow with no condition - it is enough to proeed
		} else if len(flows) == 1 {
			ret = append(ret, flow)
		}
	}
	if len(ret) == 0 {
		if defaultFlow == nil {
			return nil, &ExpressionEvaluationError{
				Msg: fmt.Sprintf("No default flow, nor matching expressions found for gateway: %s", flows[0].GetSourceRef().GetId()),
				Err: nil,
			}
		}
		ret = append(ret, defaultFlow)
	}
	return ret, nil
}
