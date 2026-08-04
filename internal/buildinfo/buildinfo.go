// Package buildinfo provides application version and source commit information.
package buildinfo

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

const (
	unknownVersion = "unknown"
	unknownCommit  = "unknown"
)

var (
	version string
	commit  = unknownCommit
)

// Info identifies the source used to build the application binary.
type Info struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

// Current returns the application version and source commit available to this binary.
func Current() (Info, error) {
	resolvedVersion, err := currentVersion()
	return Info{
		Version: resolvedVersion,
		Commit:  currentCommit(),
	}, err
}

func currentVersion() (string, error) {
	if version != "" {
		return version, nil
	}
	specJSON, err := public.GetSpecJSON()
	return resolveVersion(version, specJSON, err)
}

func resolveVersion(injectedVersion string, specJSON []byte, specErr error) (string, error) {
	if injectedVersion != "" {
		return injectedVersion, nil
	}
	if specErr != nil {
		return unknownVersion, fmt.Errorf("read embedded OpenAPI specification: %w", specErr)
	}

	var spec struct {
		Info struct {
			Version string `json:"version"`
		} `json:"info"`
	}
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return unknownVersion, fmt.Errorf("decode embedded OpenAPI specification: %w", err)
	}
	if spec.Info.Version == "" {
		return unknownVersion, fmt.Errorf("embedded OpenAPI specification has no info.version")
	}
	return spec.Info.Version, nil
}

func currentCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return commit
	}
	return resolveCommit(commit, info.Settings)
}

func resolveCommit(injectedCommit string, settings []debug.BuildSetting) string {
	if injectedCommit != unknownCommit {
		return injectedCommit
	}
	for _, setting := range settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return setting.Value
		}
	}
	return unknownCommit
}
