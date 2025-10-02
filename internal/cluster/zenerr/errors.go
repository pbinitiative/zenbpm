package zenerr

import "errors"

var (
	// ErrNotOpen is returned when a Store is not open.
	ErrNotOpen = errors.New("store not open")

	// ErrAlreadyOpen is returned when a Store is already open.
	ErrAlreadyOpen = errors.New("store already open")

	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrNodeNotFound is returned when requested node is not found in the cluster.
	ErrNodeNotFound = errors.New("node not found")

	// ErrWaitForLeaderTimeout is returned when the Store cannot determine the leader
	// within the specified time.
	ErrWaitForLeaderTimeout = errors.New("timeout waiting for leader")
)
