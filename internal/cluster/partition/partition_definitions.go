package partition

import (
	"context"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
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

// ListProcessDefinitionVersions lists the versions of the process (of every
// process when processID is empty) this partition holds, ordered by process
// id and version. With latestData the BPMN bytes of the latest version of
// every listed process are included.
func (rq *DB) ListProcessDefinitionVersions(ctx context.Context, processID string, latestData bool) ([]ProcessDefinitionVersion, error) {
	versions, err := rq.listProcessDefinitionVersions(ctx, processID)
	if err != nil {
		return nil, err
	}
	if !latestData {
		return versions, nil
	}
	latest := map[string]int{}
	for i, version := range versions {
		if index, ok := latest[version.ProcessID]; !ok || versions[index].Version < version.Version {
			latest[version.ProcessID] = i
		}
	}
	for _, index := range latest {
		row := rq.QueryRowContext(ctx, "SELECT bpmn_data FROM process_definition WHERE key = ?", versions[index].Key)
		var data string
		if err := row.Scan(&data); err != nil {
			return nil, fmt.Errorf("failed to load process definition %d: %w", versions[index].Key, err)
		}
		versions[index].Data = []byte(data)
	}
	return versions, nil
}

func (rq *DB) listProcessDefinitionVersions(ctx context.Context, processID string) (versions []ProcessDefinitionVersion, err error) {
	query := "SELECT key, version, bpmn_process_id, bpmn_checksum, version_tag FROM process_definition"
	var args []interface{}
	if processID != "" {
		query += " WHERE bpmn_process_id = ?"
		args = append(args, processID)
	}
	rows, err := rq.QueryContext(ctx, query+" ORDER BY bpmn_process_id, version", args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list process definition versions: %w", err)
	}
	defer zenerr.CloseJoin(rows, &err, "process definition rows")
	for rows.Next() {
		var version ProcessDefinitionVersion
		var number int64
		if err := rows.Scan(&version.Key, &number, &version.ProcessID, &version.Checksum, &version.VersionTag); err != nil {
			return nil, fmt.Errorf("failed to list process definition versions: %w", err)
		}
		version.Version = int32(number) // #nosec G115 -- definition versions are small counters
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list process definition versions: %w", err)
	}
	return versions, nil
}
