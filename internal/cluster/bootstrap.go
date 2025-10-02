package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/pbinitiative/zenbpm/internal/cluster/client"
	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/rqlite/rqlite/v8/cluster"
	"github.com/rqlite/rqlite/v8/random"
)

const (
	requestTimeout    = 5 * time.Second
	numJoinAttempts   = 1
	bootCheckInterval = 1 * time.Second
)

// AddressProvider is the interface types must implement to provide
// addresses to a Bootstrapper.
type AddressProvider interface {
	Lookup() ([]string, error)
}

// Bootstrapper performs a bootstrap of this node.
type Bootstrapper struct {
	provider AddressProvider

	clientMgr *client.ClientManager

	logger   hclog.Logger
	Interval time.Duration

	bootStatusMu sync.RWMutex
	bootStatus   cluster.BootStatus

	// White-box testing only
	nBootCanceled int
}

// NewBootstrapper returns an instance of a Bootstrapper.
func NewBootstrapper(p AddressProvider, client *client.ClientManager) *Bootstrapper {
	bs := &Bootstrapper{
		provider:  p,
		clientMgr: client,
		logger:    hclog.Default().Named("cluster-bootstrap"),
		Interval:  bootCheckInterval,
	}
	return bs
}

// Boot performs the bootstrapping process for this node. This means it will
// ensure this node becomes part of a cluster. It does this by either:
//   - joining an existing cluster by explicitly joining it through a node returned
//     by the AddressProvider, or
//   - if it's a Voting node, notifying all nodes returned by the AddressProvider
//     that it exists, potentially allowing a cluster-wide bootstrap take place
//     which will include this node.
//
// Returns nil if the boot operation was successful, or if done() ever returns
// true. done() is periodically polled by the boot process. Returns an error
// the boot process encounters an unrecoverable error, or booting does not
// occur within the given timeout. If booting was canceled, ErrBootCanceled is
// returned unless done() returns true at the time of cancelation, in which case
// no error is returned.
//
// id and raftAddr are those of the node calling Boot. suf is whether this node
// is a Voter or NonVoter.
func (b *Bootstrapper) Boot(ctx context.Context, id, raftAddr string, suf cluster.Suffrage, done func() bool, timeout time.Duration) error {
	timeoutT := time.NewTimer(timeout)
	defer timeoutT.Stop()
	tickerT := time.NewTimer(random.Jitter(time.Millisecond)) // Check fast, just once at the start.
	defer tickerT.Stop()

	joiner := NewJoiner(b.clientMgr, numJoinAttempts, requestTimeout)
	for {
		select {
		case <-ctx.Done():
			b.nBootCanceled++
			if done() {
				b.logger.Info("boot operation marked done")
				b.setBootStatus(cluster.BootDone)
				return nil
			}
			b.setBootStatus(cluster.BootCanceled)
			return cluster.ErrBootCanceled

		case <-timeoutT.C:
			b.setBootStatus(cluster.BootTimeout)
			return cluster.ErrBootTimeout

		case <-tickerT.C:
			if done() {
				b.logger.Info("boot operation marked done")
				b.setBootStatus(cluster.BootDone)
				return nil
			}
			tickerT.Reset(random.Jitter(b.Interval)) // Move to longer-period polling

			targets, err := b.provider.Lookup()
			if err != nil {
				b.logger.Warn(fmt.Sprintf("provider lookup failed %s", err.Error()))
			}
			if len(targets) == 0 {
				continue
			}

			// Try an explicit join first. Joining an existing cluster is always given priority
			// over trying to form a new cluster.
			if j, err := joiner.Do(ctx, targets, id, raftAddr, suf); err == nil {
				b.logger.Info(fmt.Sprintf("succeeded directly joining cluster via node at %s as %s", j, suf))
				b.setBootStatus(cluster.BootJoin)
				return nil
			}

			if suf.IsVoter() {
				// This is where we have to be careful. This node failed to join with any node
				// in the targets list. This could be because none of the nodes are contactable,
				// or none of the nodes are in a functioning cluster with a leader. That means that
				// this node could be part of a set nodes that are bootstrapping to form a cluster
				// de novo. For that to happen it needs to now let the other nodes know it is here.
				// If this is a new cluster, some node will then reach the bootstrap-expect value
				// first, form the cluster, beating all other nodes to it.
				if err := b.notify(targets, id, raftAddr); err != nil {
					b.logger.Warn(fmt.Sprintf("failed to notify all targets: %s (%s, will retry)", targets,
						err.Error()))
				} else {
					b.logger.Info(fmt.Sprintf("succeeded notifying all targets: %s", targets))
				}
			}
		}
	}
}

// Status returns the reason for the boot process completing.
func (b *Bootstrapper) Status() cluster.BootStatus {
	b.bootStatusMu.RLock()
	defer b.bootStatusMu.RUnlock()
	return b.bootStatus
}

func (b *Bootstrapper) notify(targets []string, id, raftAddr string) error {
	nr := &proto.NotifyRequest{
		Address: &raftAddr,
		Id:      &id,
	}
	for _, t := range targets {
		client, err := b.clientMgr.For(t)
		if err != nil {
			return fmt.Errorf("failed to create client for %s: %s", t, err)
		}
		timeoutCtx, cancelCtx := context.WithTimeout(context.Background(), requestTimeout)
		_, err = client.Notify(timeoutCtx, nr)
		cancelCtx()
		if err != nil {
			return fmt.Errorf("failed to notify node at %s: %s", t, err)
		}
	}
	return nil
}

func (b *Bootstrapper) setBootStatus(status cluster.BootStatus) {
	b.bootStatusMu.Lock()
	defer b.bootStatusMu.Unlock()
	b.bootStatus = status
}

type stringAddressProvider struct {
	ss []string
}

func (s *stringAddressProvider) Lookup() ([]string, error) {
	return s.ss, nil
}

// NewAddressProviderString wraps an AddressProvider around a string slice.
func NewAddressProviderString(ss []string) AddressProvider {
	return &stringAddressProvider{ss}
}
