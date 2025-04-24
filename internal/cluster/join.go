package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/rqlite/rqlite/v8/cluster"
)

var (
	// ErrNodeIDRequired is returned a join request doesn't supply a node ID
	ErrNodeIDRequired = errors.New("node required")

	// ErrJoinFailed is returned when a node fails to join a cluster
	ErrJoinFailed = errors.New("failed to join cluster")

	// ErrJoinCanceled is returned when a join operation is canceled
	ErrJoinCanceled = errors.New("join operation canceled")

	// ErrNotifyFailed is returned when a node fails to notify another node
	ErrNotifyFailed = errors.New("failed to notify node")
)

// Joiner executes a node-join operation.
type Joiner struct {
	numAttempts     int
	attemptInterval time.Duration

	clientMgr *client.ClientManager
	logger    hclog.Logger
}

// NewJoiner returns an instantiated Joiner.
func NewJoiner(clientMgr *client.ClientManager, numAttempts int, attemptInterval time.Duration) *Joiner {
	return &Joiner{
		clientMgr:       clientMgr,
		numAttempts:     numAttempts,
		attemptInterval: attemptInterval,
		logger:          hclog.Default().Named("cluster-join"),
	}
}

// Do makes the actual join request. If the join is successful with any address,
// that address is returned. Otherwise, an error is returned.
func (j *Joiner) Do(ctx context.Context, targetAddrs []string, id, addr string, suf cluster.Suffrage) (string, error) {
	if id == "" {
		return "", ErrNodeIDRequired
	}

	var err error
	var joinee string
	for i := range j.numAttempts {
		for _, ta := range targetAddrs {
			select {
			case <-ctx.Done():
				return "", ErrJoinCanceled
			default:
				joinee, err = j.join(ta, id, addr, suf)
				if err == nil {
					return joinee, nil
				}
				j.logger.Error(fmt.Sprintf("failed to join via node at %s: %s", ta, err))
			}
		}
		if i+1 < j.numAttempts {
			// This logic message only make sense if performing more than 1 join-attempt.
			j.logger.Error(fmt.Sprintf("failed to join cluster at %s, sleeping %s before retry", targetAddrs, j.attemptInterval))
			select {
			case <-ctx.Done():
				return "", ErrJoinCanceled
			case <-time.After(j.attemptInterval):
				continue // Proceed with the next attempt
			}
		}
	}
	j.logger.Error(fmt.Sprintf("failed to join cluster at %s, after %d attempt(s)", targetAddrs, j.numAttempts))
	return "", ErrJoinFailed
}

func (j *Joiner) join(targetAddr, id, addr string, suf cluster.Suffrage) (string, error) {
	req := &proto.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   suf.IsVoter(),
	}

	// Attempt to join.
	client, err := j.clientMgr.For(targetAddr)
	if err != nil {
		j.logger.Error(fmt.Sprintf("failed to create client for %s: %s", targetAddr, err))
		return "", fmt.Errorf("failed to create client for %s: %w", targetAddr, err)
	}
	timeoutCtx, cancelCtx := context.WithTimeout(context.Background(), time.Second)
	_, err = client.Join(timeoutCtx, req)
	cancelCtx()
	if err != nil {
		return "", fmt.Errorf("failed to call Join on %s: %w", targetAddr, err)
	}
	return targetAddr, nil
}
