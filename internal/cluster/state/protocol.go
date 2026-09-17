package state

import (
	"fmt"
	"slices"
)

// The protocol version numbers the command set of the main raft log. A node
// running a binary that does not know a command stops when the log delivers
// one (the FSM refuses to apply what it cannot interpret rather than diverge),
// so the leader commits a command only once every member announced a
// protocol version that includes it. Every node announces
// CurrentProtocolVersion about itself after it starts (see Node.ProtocolVersion).
//
// Add a constant for every command (or command revision) that older binaries
// cannot apply, and raise CurrentProtocolVersion to it.
const (
	// ProtocolVersionProcessDefinitionAllocation introduced the
	// ProcessDefinitionAllocation command and, with it, the announcement of
	// protocol versions; binaries before it announce nothing.
	ProtocolVersionProcessDefinitionAllocation int32 = 1

	// CurrentProtocolVersion is the protocol version this binary implements.
	CurrentProtocolVersion = ProtocolVersionProcessDefinitionAllocation
)

// MemberProtocolVersionError reports that a cluster member has not announced
// a protocol version that includes a command the leader was asked to commit.
// Reported is zero when the member has announced no version at all.
type MemberProtocolVersionError struct {
	Member   string
	Reported int32
	Required int32
}

func (e *MemberProtocolVersionError) Error() string {
	if e.Reported == 0 {
		return fmt.Sprintf("cluster member %s has not announced its protocol version (protocol version %d is required): it is still starting, or runs a binary that predates cluster-wide process definition allocation; the command cannot be committed until every member runs a binary that knows it",
			e.Member, e.Required)
	}
	return fmt.Sprintf("cluster member %s runs protocol version %d, %d is required: the command cannot be committed until every member runs a binary that knows it",
		e.Member, e.Reported, e.Required)
}

// Unannounced reports whether the member has announced no protocol version
// at all, which a member that is still starting resolves on its own; a
// member that announced an older version does not.
func (e *MemberProtocolVersionError) Unannounced() bool {
	return e.Reported == 0
}

// RequireProtocolVersion checks that every one of the members (the ids of
// the servers in the raft configuration, which all apply the log) announced
// a protocol version of at least required. It returns the
// *MemberProtocolVersionError of the first member, in id order, that did
// not: a member the state does not know counts as one that announced
// nothing.
func (c Cluster) RequireProtocolVersion(members []string, required int32) error {
	ordered := slices.Clone(members)
	slices.Sort(ordered)
	for _, member := range ordered {
		reported := c.Nodes[member].ProtocolVersion
		if reported < required {
			return &MemberProtocolVersionError{Member: member, Reported: reported, Required: required}
		}
	}
	return nil
}
