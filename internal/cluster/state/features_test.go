package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCommonFeaturesListsWhatEveryMemberAnnounced(t *testing.T) {
	c := Cluster{Nodes: map[string]Node{
		"new-1":   {Id: "new-1", Features: []string{"b", "a", "c"}},
		"new-2":   {Id: "new-2", Features: []string{"a", "b"}},
		"old":     {Id: "old"},
		"partial": {Id: "partial", Features: []string{"b"}},
	}}

	assert.Equal(t, []string{"a", "b"}, c.CommonFeatures([]string{"new-1", "new-2"}), "sorted, and only what both carry")
	assert.Equal(t, []string{"b"}, c.CommonFeatures([]string{"new-2", "partial", "new-1"}))
	assert.Empty(t, c.CommonFeatures([]string{"new-1", "old"}), "a member running an older binary announced nothing")
	assert.Empty(t, c.CommonFeatures([]string{"new-1", "unknown"}), "a member the state does not know carries nothing")
	assert.NotNil(t, c.CommonFeatures(nil), "no members carry no features, as an empty list")
	assert.Empty(t, c.CommonFeatures(nil))
}

func TestSetFeaturesCountsOnlyChanges(t *testing.T) {
	var node Node
	node.SetFeatures([]string{"a", "b"})
	assert.Equal(t, uint64(1), node.FeatureChanges)
	node.SetFeatures([]string{"b", "a"})
	assert.Equal(t, uint64(1), node.FeatureChanges, "the same features in another order are no change")
	node.SetFeatures(nil)
	node.SetFeatures([]string{"a", "b"})
	assert.Equal(t, uint64(3), node.FeatureChanges, "features lost and regained count twice, though they read the same")
	assert.Equal(t, []string{"a", "b"}, node.Features)
	node.SetFeatures(nil)
	node.SetFeatures([]string{})
	assert.Equal(t, uint64(4), node.FeatureChanges, "no features and an empty list are the same")
}

func TestCurrentFeaturesNameTheStartEventListener(t *testing.T) {
	assert.ElementsMatch(t, []string{"executionListener.startEventEnd"}, CurrentFeatures())
	assert.True(t, Node{Features: CurrentFeatures()}.AnnouncedCurrentFeatures())
	assert.False(t, Node{Features: []string{"another"}}.AnnouncedCurrentFeatures())
	assert.False(t, Node{Features: append(CurrentFeatures(), "another")}.AnnouncedCurrentFeatures(),
		"a node restarted on a binary with fewer features announces again")
	assert.False(t, Node{}.AnnouncedCurrentFeatures())
}
