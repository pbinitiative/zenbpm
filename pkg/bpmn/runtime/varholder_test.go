package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExecutionScopeSnapshotIncludesAllAncestors(t *testing.T) {
	grandparent := NewVariableHolder(nil, map[string]interface{}{
		"grandparent": "grandparent",
		"shared":      "grandparent",
	})
	parent := NewVariableHolder(&grandparent, map[string]interface{}{
		"parent": "parent",
		"shared": "parent",
	})
	child := NewVariableHolder(&parent, map[string]interface{}{
		"child":  "child",
		"shared": "child",
	})

	assert.Equal(t, map[string]interface{}{
		"grandparent": "grandparent",
		"parent":      "parent",
		"child":       "child",
		"shared":      "child",
	}, child.ExecutionScopeSnapshot())
}
