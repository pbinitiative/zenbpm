package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLastContactMillis(t *testing.T) {
	tests := []struct {
		name     string
		stats    map[string]string
		expected int64
	}{
		{name: "missing stat", stats: map[string]string{}, expected: -1},
		{name: "never contacted", stats: map[string]string{"last_contact": "never"}, expected: -1},
		{name: "node is the leader", stats: map[string]string{"last_contact": "0"}, expected: 0},
		{name: "sub millisecond contact", stats: map[string]string{"last_contact": "500µs"}, expected: 0},
		{name: "millisecond contact", stats: map[string]string{"last_contact": "12.5ms"}, expected: 12},
		{name: "second contact", stats: map[string]string{"last_contact": "1.5s"}, expected: 1500},
		{name: "unparsable value", stats: map[string]string{"last_contact": "soon"}, expected: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, lastContactMillis(tt.stats))
		})
	}
}
