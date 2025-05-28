package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
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
