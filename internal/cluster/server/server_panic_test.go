package server

import (
	"context"
	"testing"

	protoc "github.com/pbinitiative/zenbpm/internal/cluster/command/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/stretchr/testify/assert"
)

func TestNodeCommandUnexpectedTypeReturnsError(t *testing.T) {
	s := &Server{}

	var (
		resp *proto.NodeCommandResponse
		err  error
	)

	assert.NotPanics(t, func() {
		resp, err = s.NodeCommand(context.Background(), &protoc.Command{
			Type: protoc.Command_TYPE_NOOP.Enum(),
		})
	})

	assert.Error(t, err)
	assert.Nil(t, resp)
}
