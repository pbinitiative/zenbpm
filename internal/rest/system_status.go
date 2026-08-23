package rest

import (
	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

const shortCommitIDLength = 12

type systemStatusResponse struct {
	Git   systemStatusGit   `json:"git"`
	Build systemStatusBuild `json:"build"`
	state.Cluster
}

type systemStatusGit struct {
	Branch   string `json:"branch"`
	CommitID string `json:"commitId"`
}

type systemStatusBuild struct {
	Version string `json:"version"`
	Time    string `json:"time"`
}

func newSystemStatusResponse(info buildinfo.Info, cluster state.Cluster) systemStatusResponse {
	return systemStatusResponse{
		Git: systemStatusGit{
			Branch:   info.Branch,
			CommitID: shortCommitID(info.Commit),
		},
		Build: systemStatusBuild{
			Version: info.Version,
			Time:    info.BuildTime,
		},
		Cluster: cluster,
	}
}

func shortCommitID(commit string) string {
	if len(commit) <= shortCommitIDLength {
		return commit
	}
	return commit[:shortCommitIDLength]
}
