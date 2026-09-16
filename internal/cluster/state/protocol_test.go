package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequireProtocolVersionChecksEveryMember(t *testing.T) {
	c := Cluster{Nodes: map[string]Node{
		"a": {Id: "a", ProtocolVersion: 2},
		"b": {Id: "b", ProtocolVersion: 1},
		"c": {Id: "c"},
	}}

	require.NoError(t, c.RequireProtocolVersion([]string{"a", "b"}, 1))
	require.NoError(t, c.RequireProtocolVersion(nil, 1))

	// a member that announced an older version
	err := c.RequireProtocolVersion([]string{"b", "a"}, 2)
	var member *MemberProtocolVersionError
	require.ErrorAs(t, err, &member)
	assert.Equal(t, &MemberProtocolVersionError{Member: "b", Reported: 1, Required: 2}, member)
	assert.False(t, member.Unannounced())
	assert.ErrorContains(t, err, "runs protocol version 1, 2 is required")

	// a member that announced nothing, and one the state does not know at all
	for _, unannounced := range []string{"c", "unknown"} {
		err = c.RequireProtocolVersion([]string{"a", unannounced}, 1)
		require.ErrorAs(t, err, &member)
		assert.Equal(t, unannounced, member.Member)
		assert.True(t, member.Unannounced())
		assert.ErrorContains(t, err, "has not announced its protocol version")
	}

	// the first failing member in id order is reported, whatever the order given
	err = c.RequireProtocolVersion([]string{"c", "b"}, 2)
	require.ErrorAs(t, err, &member)
	assert.Equal(t, "b", member.Member)
}
