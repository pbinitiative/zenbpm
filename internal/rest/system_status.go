package rest

import (
	"github.com/pbinitiative/zenbpm/internal/buildinfo"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

const shortCommitIDLength = 12

type systemStatusResponse struct {
	Git    systemStatusGit     `json:"git"`
	Build  systemStatusBuild   `json:"build"`
	Health clusterStatusHealth `json:"health"`
	// Features are the features every member of the cluster carries (see
	// state.CurrentFeatures and cluster.ZenNode.Features). A client asks for a feature here instead of
	// comparing versions: the answering node's own build says nothing of the
	// nodes leading the partitions.
	Features []string `json:"features"`
	clusterStatusView
}

type systemStatusGit struct {
	Branch   string `json:"branch"`
	CommitID string `json:"commitId"`
}

type systemStatusBuild struct {
	Version string `json:"version"`
	Time    string `json:"time"`
}

func newSystemStatusResponse(info buildinfo.Info, cluster state.Cluster, features []string, healthy bool, reasons []string) systemStatusResponse {
	if features == nil {
		features = []string{}
	}
	if reasons == nil {
		reasons = []string{}
	}
	return systemStatusResponse{
		Git: systemStatusGit{
			Branch:   info.Branch,
			CommitID: shortCommitID(info.Commit),
		},
		Build: systemStatusBuild{
			Version: info.Version,
			Time:    info.BuildTime,
		},
		Health:            clusterStatusHealth{OK: healthy, Reasons: reasons},
		Features:          features,
		clusterStatusView: buildClusterStatusView(cluster),
	}
}

func shortCommitID(commit string) string {
	if len(commit) <= shortCommitIDLength {
		return commit
	}
	return commit[:shortCommitIDLength]
}
