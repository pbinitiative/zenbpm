package dmn

import (
	"context"
	"strings"

	dmnModel "github.com/pbinitiative/zenbpm/pkg/dmn/model/dmn"
	"github.com/pbinitiative/zenbpm/pkg/dmn/runtime"
	"github.com/pbinitiative/zenbpm/pkg/validation"
)

func (engine *ZenDmnEngine) Validate(_ context.Context, dmnDefinition *runtime.DmnResourceDefinition) error {
	for _, decision := range dmnDefinition.Definitions.Decisions {
		if decision.DecisionTable != nil {
			if err := engine.validateDecisionTable(decision.Id, decision.DecisionTable); err != nil {
				return err
			}
		}

		if decision.Context != nil {
			if err := engine.validateContextDecisionTables(decision.Id, decision.Context); err != nil {
				return err
			}
		}
	}

	return nil
}

func (engine *ZenDmnEngine) validateContextDecisionTables(decisionID string, decisionContext *dmnModel.TContext) error {
	for _, contextEntry := range decisionContext.ContextEntries {
		if contextEntry.DecisionTable != nil {
			if err := engine.validateDecisionTable(decisionID, contextEntry.DecisionTable); err != nil {
				return err
			}
		}

		if contextEntry.Context != nil {
			if err := engine.validateContextDecisionTables(decisionID, contextEntry.Context); err != nil {
				return err
			}
		}
	}

	return nil
}

func (engine *ZenDmnEngine) validateDecisionTable(decisionID string, decisionTable *dmnModel.TDecisionTable) error {
	feelRuntime, err := engine.decisionTableFeelRuntime()
	if err != nil {
		return validation.Errorf("decision %q cannot be validated: %v", decisionID, err)
	}

	for _, input := range decisionTable.Inputs {
		expression := normalizeFeelStringLiteral(input.InputExpression.Text)
		if err := feelRuntime.ValidateExpression(expression); err != nil {
			return validation.Errorf(
				"decision %q input %q expression %q contains invalid or unsupported FEEL expression %q: %v",
				decisionID,
				input.Id,
				input.InputExpression.Id,
				expression,
				err,
			)
		}
	}

	for _, rule := range decisionTable.Rules {
		if len(rule.InputEntry) != len(decisionTable.Inputs) {
			return validation.Errorf(
				"decision %q rule %q has %d input entries, expected %d",
				decisionID,
				rule.Id,
				len(rule.InputEntry),
				len(decisionTable.Inputs),
			)
		}

		if len(rule.OutputEntry) != len(decisionTable.Outputs) {
			return validation.Errorf(
				"decision %q rule %q has %d output entries, expected %d",
				decisionID,
				rule.Id,
				len(rule.OutputEntry),
				len(decisionTable.Outputs),
			)
		}

		for _, inputEntry := range rule.InputEntry {
			expression := normalizeUnaryTestExpression(inputEntry.Text)

			if err := feelRuntime.ValidateUnaryTest(expression); err != nil {
				return validation.Errorf(
					"decision %q rule %q input entry %q contains invalid or unsupported FEEL unary test %q: %v",
					decisionID,
					rule.Id,
					inputEntry.Id,
					expression,
					err,
				)
			}
		}

		for _, outputEntry := range rule.OutputEntry {
			expression := normalizeFeelStringLiteral(outputEntry.Text)
			if err := feelRuntime.ValidateExpression(expression); err != nil {
				return validation.Errorf(
					"decision %q rule %q output entry %q contains invalid or unsupported FEEL expression %q: %v",
					decisionID,
					rule.Id,
					outputEntry.Id,
					expression,
					err,
				)
			}
		}
	}

	return nil
}

func normalizeUnaryTestExpression(expression string) string {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return "-"
	}
	return expression
}
