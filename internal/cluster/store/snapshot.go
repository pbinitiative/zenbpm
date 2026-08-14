package store

import (
	"encoding/json"
	"errors"

	"github.com/hashicorp/raft"
	"github.com/pbinitiative/zenbpm/internal/cluster/state"
)

type fsmSnapshot struct {
	ClusterState state.Cluster `json:"clusterState"`
}

var _ raft.FSMSnapshot = &fsmSnapshot{}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		if cancelErr := sink.Cancel(); cancelErr != nil {
			return errors.Join(err, cancelErr)
		}
	}

	return err
}

func (f *fsmSnapshot) Release() {}
