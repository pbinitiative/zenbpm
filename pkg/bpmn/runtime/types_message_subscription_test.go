package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func baseMessageSubscriptionData() MessageSubscriptionData {
	return MessageSubscriptionData{
		Key:                  1,
		ElementId:            "elem-1",
		Name:                 "order_received",
		State:                ActivityStateActive,
		ProcessDefinitionKey: 100,
		CreatedAt:            time.Now().Truncate(time.Millisecond),
	}
}

func TestEqualTo_EqualSubscriptions(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	m2 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	assert.True(t, EqualTo(m1, m2), "TokenMessageSubscriptions with identical fields should be equal")
}

func TestEqualTo_TokenDifferentProcessInstanceKey(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	m2 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      99, // differs
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	assert.False(t, EqualTo(m1, m2), "different ProcessInstanceKey should make TokenMessageSubscriptions unequal")
}

func TestEqualTo_TokenDifferentCorrelationKey(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	m2 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-different",
		MessageSubscriptionData: data,
	}
	assert.False(t, EqualTo(m1, m2), "different CorrelationKey should make TokenMessageSubscriptions unequal")
}

func TestEqualTo_TokenDifferentTokenKey(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 10},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	m2 := &TokenMessageSubscription{
		Token:                   ExecutionToken{Key: 99},
		ProcessInstanceKey:      42,
		CorrelationKey:          "corr-1",
		MessageSubscriptionData: data,
	}
	assert.False(t, EqualTo(m1, m2), "different Token.Key should make TokenMessageSubscriptions unequal")
}

func TestEqualTo_InstanceEqual(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &InstanceMessageSubscription{ProcessInstanceKey: 1, CorrelationKey: "ck", MessageSubscriptionData: data}
	m2 := &InstanceMessageSubscription{ProcessInstanceKey: 1, CorrelationKey: "ck", MessageSubscriptionData: data}
	assert.True(t, EqualTo(m1, m2))
}

func TestEqualTo_InstanceDifferentProcessInstanceKey(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &InstanceMessageSubscription{ProcessInstanceKey: 1, CorrelationKey: "ck", MessageSubscriptionData: data}
	m2 := &InstanceMessageSubscription{ProcessInstanceKey: 2, CorrelationKey: "ck", MessageSubscriptionData: data}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_DefinitionEqual(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &DefinitionMessageSubscription{MessageSubscriptionData: data}
	m2 := &DefinitionMessageSubscription{MessageSubscriptionData: data}
	assert.True(t, EqualTo(m1, m2))
}

func TestEqualTo_NilHandling(t *testing.T) {
	data := baseMessageSubscriptionData()
	m := &DefinitionMessageSubscription{MessageSubscriptionData: data}
	assert.False(t, EqualTo(nil, m))
	assert.False(t, EqualTo(m, nil))
	assert.True(t, EqualTo(nil, nil))
}

func TestEqualTo_DifferentKey(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.Key = 999
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2), "subscriptions with different Keys should not be equal")
}

func TestEqualTo_DifferentElementId(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.ElementId = "elem-2"
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_DifferentName(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.Name = "other_event"
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_DifferentState(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.State = ActivityStateCompleted
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_DifferentProcessDefinitionKey(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.ProcessDefinitionKey = 200
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_DifferentCreatedAt(t *testing.T) {
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d2.CreatedAt = d1.CreatedAt.Add(time.Second)
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.False(t, EqualTo(m1, m2))
}

func TestEqualTo_CreatedAtSubMillisecondDifference(t *testing.T) {
	// Sub-millisecond differences should be ignored (both are truncated)
	base := time.Now().Truncate(time.Millisecond)
	d1 := baseMessageSubscriptionData()
	d2 := baseMessageSubscriptionData()
	d1.CreatedAt = base
	d2.CreatedAt = base.Add(500 * time.Microsecond) // less than 1ms
	m1 := &TokenMessageSubscription{MessageSubscriptionData: d1}
	m2 := &TokenMessageSubscription{MessageSubscriptionData: d2}
	assert.True(t, EqualTo(m1, m2), "sub-millisecond CreatedAt difference should be ignored")
}

func TestEqualTo_DifferentConcreteTypes(t *testing.T) {
	// EqualTo distinguishes between subscription kinds
	data := baseMessageSubscriptionData()
	m1 := &TokenMessageSubscription{MessageSubscriptionData: data}
	m2 := &InstanceMessageSubscription{MessageSubscriptionData: data}
	assert.False(t, EqualTo(m1, m2), "different concrete types must not be equal")
}

// unknownMessageSubscription is a type that satisfies the MessageSubscription interface
// but is not one of the three known concrete types, exercising the default fallback.
type unknownMessageSubscription struct {
	MessageSubscriptionData
}

func (u *unknownMessageSubscription) Type() MessageSubscriptionType {
	return MessageSubscriptionType(999)
}
func (u *unknownMessageSubscription) MessageSubscription() *MessageSubscriptionData {
	return &u.MessageSubscriptionData
}

func TestEqualTo_UnknownTypeFallbackReturnsFalse(t *testing.T) {
	data := baseMessageSubscriptionData()
	m1 := &unknownMessageSubscription{MessageSubscriptionData: data}
	m2 := &unknownMessageSubscription{MessageSubscriptionData: data}
	assert.False(t, EqualTo(m1, m2), "unknown concrete type should return false")
}
