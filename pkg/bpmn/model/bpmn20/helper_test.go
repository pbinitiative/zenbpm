package bpmn20

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_no_expression_when_only_blanks(t *testing.T) {
	// given
	flow := TSequenceFlow{
		ConditionExpression: []TExpression{
			{Text: "   "},
		},
	}
	// when
	result := flow.HasConditionExpression()
	// then
	assert.False(t, result)
}

func Test_has_expression_when_some_characters_present(t *testing.T) {
	// given
	flow := TSequenceFlow{
		ConditionExpression: []TExpression{
			{Text: " x>y "},
		},
	}
	// when
	result := flow.HasConditionExpression()
	// then
	assert.True(t, result)
}
