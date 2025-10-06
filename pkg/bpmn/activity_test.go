package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
)

func TestActivityInterfacesImplemented(t *testing.T) {
	var _ runtime.Activity = &elementActivity{}
}

func TestGatewayActivityInterfacesImplemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}

func TestEventBaseGatewayActivityInterfacesImplemented(t *testing.T) {
	var _ runtime.Activity = &gatewayActivity{}
}
