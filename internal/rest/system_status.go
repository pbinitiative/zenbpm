package rest

import (
	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

type systemStatusResponse struct {
	buildinfo.Info
	state.Cluster
}

func newSystemStatusResponse(info buildinfo.Info, cluster state.Cluster) systemStatusResponse {
	return systemStatusResponse{
		Info:    info,
		Cluster: cluster,
	}
}
