package partition

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/sql"
)

// ProcessDefinitionVersion is one stored version of a BPMN process.
type ProcessDefinitionVersion struct {
	ProcessID  string
	Key        int64
	Version    int32
	Checksum   []byte
	VersionTag string
	// Data holds the BPMN bytes when they were asked for; only the latest
	// version of a process carries them.
	Data []byte
}

// ProcessDefinitionVersionsQuery selects what ListProcessDefinitionVersions answers.
type ProcessDefinitionVersionsQuery struct {
	// ProcessID restricts the answer to one process; empty lists every process.
	ProcessID string
	// LatestData includes the BPMN bytes of the latest version of every listed process.
	LatestData bool
	// LatestAndTaggedOnly restricts the answer to the latest version of every
	// process and the versions carrying a version tag.
	LatestAndTaggedOnly bool
}

// ListProcessDefinitionVersions lists the versions of process definitions
// this partition holds, ordered by process id and version. The read is
// linearizable: it is answered by the partition leader only and includes
// every deployment the partition acknowledged before it, in one consistent
// statement. On a node that does not lead the partition the error wraps
// store.ErrNotLeader.
func (rq *DB) ListProcessDefinitionVersions(ctx context.Context, q ProcessDefinitionVersionsQuery) ([]ProcessDefinitionVersion, error) {
	params := sql.ListProcessDefinitionVersionsParams{
		LatestData:          sqlFlag(q.LatestData),
		LatestAndTaggedOnly: sqlFlag(q.LatestAndTaggedOnly),
	}
	if q.ProcessID != "" {
		params.BpmnProcessID = q.ProcessID
	}
	rows, err := rq.LinearizableQueries.ListProcessDefinitionVersions(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to list process definition versions: %w", err)
	}
	versions := make([]ProcessDefinitionVersion, 0, len(rows))
	for _, row := range rows {
		version := ProcessDefinitionVersion{
			ProcessID:  row.BpmnProcessID,
			Key:        row.Key,
			Version:    int32(row.Version), // #nosec G115 -- definition versions are small counters
			Checksum:   row.BpmnChecksum,
			VersionTag: row.VersionTag,
		}
		if row.BpmnData != "" {
			version.Data = []byte(row.BpmnData)
		}
		versions = append(versions, version)
	}
	return versions, nil
}

// sqlFlag is a boolean as the generated queries take it: the statement parameters carry numbers, not booleans.
func sqlFlag(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
