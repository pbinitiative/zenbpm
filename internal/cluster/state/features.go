package state

import (
	"slices"
)

// The features a client can ask the cluster for on GET /system/status, so
// that it need not compare versions. A feature is listed only when every
// member runs a binary which carries it. A feature name never changes its
// meaning, and it is never removed while the feature exists.
const (
	// FeatureExecutionListenerStartEventEnd runs an "end" execution listener
	// on a start event of a process.
	FeatureExecutionListenerStartEventEnd = "executionListener.startEventEnd"
)

// CurrentFeatures answers the features this binary carries, which a node
// announces about itself together with its protocol version.
func CurrentFeatures() []string {
	return []string{
		FeatureExecutionListenerStartEventEnd,
	}
}

// AnnouncedCurrentFeatures reports whether the node's recorded features are
// exactly the features this binary carries. A record with more of them asks
// for an announcement as well as one with fewer: a node restarted on a binary
// which carries fewer features must not keep the list of the binary before.
func (n Node) AnnouncedCurrentFeatures() bool {
	return slices.Equal(sortedFeatures(n.Features), sortedFeatures(CurrentFeatures()))
}

// SetFeatures records the node's features and, when they differ from the
// recorded ones, counts the change in FeatureChanges.
func (n *Node) SetFeatures(features []string) {
	if !slices.Equal(sortedFeatures(n.Features), sortedFeatures(features)) {
		n.FeatureChanges++
	}
	n.Features = slices.Clone(features)
}

func sortedFeatures(features []string) []string {
	return slices.Compact(slices.Sorted(slices.Values(features)))
}

// CommonFeatures answers the features every one of the members announced,
// sorted. A member the state does not know, or one which announced nothing,
// such as a node still starting or one running an older binary, leaves none:
// every doubt falls on the side of too few features.
func (c Cluster) CommonFeatures(members []string) []string {
	if len(members) == 0 {
		return []string{}
	}
	common := []string{}
	for _, feature := range c.Nodes[members[0]].Features {
		inEveryMember := true
		for _, member := range members[1:] {
			if !slices.Contains(c.Nodes[member].Features, feature) {
				inEveryMember = false
				break
			}
		}
		if inEveryMember && !slices.Contains(common, feature) {
			common = append(common, feature)
		}
	}
	slices.Sort(common)
	return common
}
