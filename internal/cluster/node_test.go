package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransientDeployError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error is not transient", nil, false},
		{"no engine available (singular)", errors.New("client call to deploy process definition simple_task.bpmn failed\nno engine available on this node"), true},
		{"no engines available (plural)", errors.New("no engines available: %!w(<nil>)"), true},
		{"store not open", errors.New("failed to deploy process definition: failed to load running tokens: store not open"), true},
		{"unrelated technical error is not transient", errors.New("failed to parse request data: invalid xml"), false},
		{"not found is not transient", errors.New("process definition not found"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, transientDeployError(tt.err))
		})
	}
}
