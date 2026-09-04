package rest

import (
	"errors"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func TestCreateProcessDefinitionRejectsUnreadableBody(t *testing.T) {
	request := public.CreateProcessDefinitionRequestObject{Body: failingReader{}}

	response, err := (&Server{}).CreateProcessDefinition(t.Context(), request)

	require.NoError(t, err)
	require.IsType(t, public.CreateProcessDefinition400JSONResponse{}, response)
}
