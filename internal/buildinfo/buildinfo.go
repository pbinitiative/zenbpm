// Package buildinfo provides application build and source information.
package buildinfo

import (
	"runtime/debug"

	"github.com/pbinitiative/zenbpm"
)

const unknown = "unknown"

var (
	commit    = unknown
	branch    = unknown
	buildTime = unknown
)

// Info identifies the source used to build the application binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Branch    string `json:"branch"`
	BuildTime string `json:"buildTime"`
}

// Current returns the build and source information available to this binary.
func Current() Info {
	var settings []debug.BuildSetting
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		settings = buildInfo.Settings
	}

	return resolveInfo(commit, branch, buildTime, settings)
}

func resolveInfo(injectedCommit string, injectedBranch string, injectedBuildTime string, settings []debug.BuildSetting) Info {
	return Info{
		Version:   zenbpm.Version(),
		Commit:    resolveCommit(injectedCommit, settings),
		Branch:    normalize(injectedBranch),
		BuildTime: normalize(injectedBuildTime),
	}
}

func resolveCommit(injectedCommit string, settings []debug.BuildSetting) string {
	if normalizedCommit := normalize(injectedCommit); normalizedCommit != unknown {
		return normalizedCommit
	}
	for _, setting := range settings {
		if setting.Key == "vcs.revision" && setting.Value != "" {
			return setting.Value
		}
	}
	return unknown
}

func normalize(value string) string {
	if value != "" && value != unknown {
		return value
	}
	return unknown
}
