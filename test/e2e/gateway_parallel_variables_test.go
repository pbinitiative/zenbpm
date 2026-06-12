package e2e

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/zenclient"
)

const (
	parallelGatewayVariablesPath             = "testdata/gateway_parallel/parallel_gateway_variables.bpmn"
	parallelGatewayServiceBranchAElementID   = "service_task_branch_a"
	parallelGatewayUserBranchBElementID      = "user_task_branch_b"
	parallelGatewayAfterJoinTaskElementID    = "service_task_after_join"
	parallelGatewayBranchASeedSeenVariable   = "branch_a_seed_seen"
	parallelGatewayBranchBSeedSeenVariable   = "branch_b_seed_seen"
	parallelGatewayBranchAValueVariable      = "branch_a_value"
	parallelGatewayBranchBValueVariable      = "branch_b_value"
	parallelGatewaySharedValueVariable       = "shared_value"
	parallelGatewayBranchASeedOutputVariable = "branch_a_seed_output"
	parallelGatewayBranchBSeedOutputVariable = "branch_b_seed_output"
)

func TestParallelGatewayVariables(t *testing.T) {
	t.Run("Variables created before split are visible in all branches", func(t *testing.T) {
		createVariables := map[string]any{
			"seed":      "created-before-split",
			"unchanged": "base",
		}
		processInstance := createParallelGatewayVariablesInstance(t, createVariables)

		waitForParallelGatewayVariableBranches(t, processInstance.Key)

		assertFlowElementInputVariables(t, processInstance.Key, parallelGatewayServiceBranchAElementID, mergeMaps(createVariables, map[string]any{
			parallelGatewayBranchASeedSeenVariable: "created-before-split",
		}))
		assertFlowElementInputVariables(t, processInstance.Key, parallelGatewayUserBranchBElementID, mergeMaps(createVariables, map[string]any{
			parallelGatewayBranchBSeedSeenVariable: "created-before-split",
		}))
	})

	t.Run("Variables written in each branch are available after join", func(t *testing.T) {
		createVariables := map[string]any{"seed": "join-seed"}
		processInstance := createParallelGatewayVariablesInstance(t, createVariables)

		waitForParallelGatewayVariableBranches(t, processInstance.Key)
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayServiceBranchAElementID, "value-a", "from-a")
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayUserBranchBElementID, "value-b", "from-b")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID)

		expectedProcessVariables := expectedParallelGatewayProcessVariables("join-seed", "value-a", "value-b", "from-b")
		assertProcessInstanceVariables(t, processInstance.Key, expectedProcessVariables)
		assertFlowElementInputVariables(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID,
			expectedParallelGatewayAfterJoinInputVariables("join-seed", "value-a", "value-b", "from-b"))
	})

	t.Run("Completion order does not change final variables", func(t *testing.T) {
		expectedProcessVariables := expectedParallelGatewayProcessVariables("order-seed", "value-a", "value-b", "same-shared")

		firstProcess := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "order-seed"})
		waitForParallelGatewayVariableBranches(t, firstProcess.Key)
		completeParallelGatewayVariableBranch(t, firstProcess.Key, parallelGatewayServiceBranchAElementID, "value-a", "same-shared")
		completeParallelGatewayVariableBranch(t, firstProcess.Key, parallelGatewayUserBranchBElementID, "value-b", "same-shared")
		waitForProcessInstanceActiveJobByElementId(t, firstProcess.Key, parallelGatewayAfterJoinTaskElementID)
		assertProcessInstanceVariables(t, firstProcess.Key, expectedProcessVariables)

		secondProcess := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "order-seed"})
		waitForParallelGatewayVariableBranches(t, secondProcess.Key)
		completeParallelGatewayVariableBranch(t, secondProcess.Key, parallelGatewayUserBranchBElementID, "value-b", "same-shared")
		completeParallelGatewayVariableBranch(t, secondProcess.Key, parallelGatewayServiceBranchAElementID, "value-a", "same-shared")
		waitForProcessInstanceActiveJobByElementId(t, secondProcess.Key, parallelGatewayAfterJoinTaskElementID)
		assertProcessInstanceVariables(t, secondProcess.Key, expectedProcessVariables)
	})

	t.Run("Two branches writing different variables are merged correctly", func(t *testing.T) {
		processInstance := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "merge-seed"})

		waitForParallelGatewayVariableBranches(t, processInstance.Key)
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayServiceBranchAElementID, "service-branch-value", "same-value")
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayUserBranchBElementID, "user-branch-value", "same-value")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID)

		assertProcessInstanceVariables(t, processInstance.Key,
			expectedParallelGatewayProcessVariables("merge-seed", "service-branch-value", "user-branch-value", "same-value"))
	})

	t.Run("Two branches writing the same variable have defined behavior - service branch then user branch", func(t *testing.T) {

		processInstance := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "same-name-seed"})

		waitForParallelGatewayVariableBranches(t, processInstance.Key)
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayServiceBranchAElementID, "value-from-service", "shared-from-service")
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayUserBranchBElementID, "value-from-user", "shared-from-user")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID)

		assertProcessInstanceVariables(t, processInstance.Key,
			expectedParallelGatewayProcessVariables("same-name-seed", "value-from-service", "value-from-user", "shared-from-user"))
	})

	t.Run("Two branches writing the same variable have defined behavior - user branch then service branch", func(t *testing.T) {

		processInstance := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "same-name-seed"})

		waitForParallelGatewayVariableBranches(t, processInstance.Key)
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayUserBranchBElementID, "value-from-user", "shared-from-user")
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayServiceBranchAElementID, "value-from-service", "shared-from-service")
		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID)

		assertProcessInstanceVariables(t, processInstance.Key,
			expectedParallelGatewayProcessVariables("same-name-seed", "value-from-service", "value-from-user", "shared-from-service"))
	})

	t.Run("Output mappings in parallel branches are applied correctly", func(t *testing.T) {
		processInstance := createParallelGatewayVariablesInstance(t, map[string]any{"seed": "mapping-seed"})

		waitForParallelGatewayVariableBranches(t, processInstance.Key)
		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayServiceBranchAElementID, "mapped-service-value", "shared-from-service")
		assertFlowElementOutputVariables(t, processInstance.Key, parallelGatewayServiceBranchAElementID,
			expectedParallelGatewayServiceBranchOutput("mapping-seed", "mapped-service-value", "shared-from-service"))

		completeParallelGatewayVariableBranch(t, processInstance.Key, parallelGatewayUserBranchBElementID, "mapped-user-value", "shared-from-user")
		assertFlowElementOutputVariables(t, processInstance.Key, parallelGatewayUserBranchBElementID,
			expectedParallelGatewayUserBranchOutput("mapping-seed", "mapped-user-value", "shared-from-user"))

		waitForProcessInstanceActiveJobByElementId(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID)
		assertFlowElementInputVariables(t, processInstance.Key, parallelGatewayAfterJoinTaskElementID,
			expectedParallelGatewayAfterJoinInputVariables("mapping-seed", "mapped-service-value", "mapped-user-value", "shared-from-user"))
	})
}

func createParallelGatewayVariablesInstance(t *testing.T, variables map[string]any) zenclient.ProcessInstance {
	t.Helper()

	processInstance := deployAndCreateUniqueProcessDefinition(t, parallelGatewayVariablesPath, variables)
	t.Cleanup(func() {
		cleanupOwnedProcessInstance(t, processInstance.Key)
	})

	return processInstance
}

func waitForParallelGatewayVariableBranches(t testing.TB, processInstanceKey int64) {
	t.Helper()

	waitForProcessInstanceActiveJobByElementId(t, processInstanceKey, parallelGatewayServiceBranchAElementID)
	waitForProcessInstanceActiveJobByElementId(t, processInstanceKey, parallelGatewayUserBranchBElementID)
}

func completeParallelGatewayVariableBranch(t testing.TB, processInstanceKey int64, elementID string, branchValue string, sharedValue string) {
	t.Helper()

	switch elementID {
	case parallelGatewayServiceBranchAElementID:
		completeJobForElementId(t, processInstanceKey, elementID, map[string]any{
			"branch_a_job": branchValue,
			"shared_job":   sharedValue,
		})
	case parallelGatewayUserBranchBElementID:
		completeJobForElementId(t, processInstanceKey, elementID, map[string]any{
			"branch_b_job": branchValue,
			"shared_job":   sharedValue,
		})
	default:
		t.Fatalf("unsupported parallel gateway variable branch %q", elementID)
	}
}

func expectedParallelGatewayProcessVariables(seed string, branchAValue string, branchBValue string, sharedValue string) map[string]any {
	return map[string]any{
		"seed":                                   seed,
		parallelGatewayBranchAValueVariable:      branchAValue,
		parallelGatewayBranchBValueVariable:      branchBValue,
		parallelGatewaySharedValueVariable:       sharedValue,
		parallelGatewayBranchASeedOutputVariable: seed,
		parallelGatewayBranchBSeedOutputVariable: seed,
	}
}

func expectedParallelGatewayAfterJoinInputVariables(seed string, branchAValue string, branchBValue string, sharedValue string) map[string]any {
	return mergeMaps(expectedParallelGatewayProcessVariables(seed, branchAValue, branchBValue, sharedValue), map[string]any{
		"after_seed":                 seed,
		"after_branch_a_value":       branchAValue,
		"after_branch_b_value":       branchBValue,
		"after_shared_value":         sharedValue,
		"after_branch_a_seed_output": seed,
		"after_branch_b_seed_output": seed,
	})
}

func expectedParallelGatewayServiceBranchOutput(seed string, branchValue string, sharedValue string) map[string]any {
	return map[string]any{
		parallelGatewayBranchAValueVariable:      branchValue,
		parallelGatewaySharedValueVariable:       sharedValue,
		parallelGatewayBranchASeedOutputVariable: seed,
	}
}

func expectedParallelGatewayUserBranchOutput(seed string, branchValue string, sharedValue string) map[string]any {
	return map[string]any{
		parallelGatewayBranchBValueVariable:      branchValue,
		parallelGatewaySharedValueVariable:       sharedValue,
		parallelGatewayBranchBSeedOutputVariable: seed,
	}
}
