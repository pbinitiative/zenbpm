package bpmn

import (
	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"testing"
)

func Test_Activity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &elementActivity{}
}

func Test_GatewayActivity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}

func Test_EventBaseGatewayActivity_interfaces_implemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}

func Test_Timer_implements_Activity(t *testing.T) {
	var _ runtime.Activity = &runtime.Timer{}
}

func Test_Job_implements_Activity(t *testing.T) {
	var _ runtime.Activity = &runtime.Job{}
}

func Test_MessageSubscription_implements_Activity(t *testing.T) {
	var _ runtime.Activity = &runtime.MessageSubscription{}
}
