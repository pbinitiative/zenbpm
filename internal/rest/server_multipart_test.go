package rest

import (
	"mime/multipart"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/require"
)

func TestCreateProcessDefinitionRejectsMalformedSkippedPart(t *testing.T) {
	const boundary = "test-boundary"
	body := strings.NewReader("--" + boundary + "\r\n" +
		"Content-Disposition: form-data; name=\"metadata\"\r\n\r\n" +
		"truncated without a closing boundary")
	request := public.CreateProcessDefinitionRequestObject{
		Body: multipart.NewReader(body, boundary),
	}

	response, err := (&Server{}).CreateProcessDefinition(t.Context(), request)

	require.NoError(t, err)
	require.IsType(t, public.CreateProcessDefinition400JSONResponse{}, response)
}
