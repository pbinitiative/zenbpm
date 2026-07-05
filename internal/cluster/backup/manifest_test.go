package backup

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func validManifest() Manifest {
	return Manifest{
		FormatVersion:   ManifestFormatVersion,
		ZenBPMVersion:   "test",
		CreatedAtMillis: 1000,
		PartitionCount:  2,
		Partitions: map[uint32]PartitionMeta{
			1: {SnapshotAtMillis: 1000, SizeBytes: 10, SHA256: "aa", SchemaVersion: "0007_process_instance_start_element_id.up.sql"},
			2: {SnapshotAtMillis: 1001, SizeBytes: 20, SHA256: "bb", SchemaVersion: "0007_process_instance_start_element_id.up.sql"},
		},
	}
}

func TestManifestJSONRoundTrip(t *testing.T) {
	m := validManifest()
	b, err := json.Marshal(m)
	assert.NoError(t, err)
	var got Manifest
	assert.NoError(t, json.Unmarshal(b, &got))
	assert.Equal(t, m, got)
}

func TestPartitionFileName(t *testing.T) {
	assert.Equal(t, "partition-3.db.gz", PartitionFileName(3))
}

func TestManifestValidate(t *testing.T) {
	binSchema := "0007_process_instance_start_element_id.up.sql"
	tests := []struct {
		name    string
		mutate  func(m *Manifest)
		count   uint32
		wantErr string
	}{
		{name: "valid", mutate: func(m *Manifest) {}, count: 2, wantErr: ""},
		{name: "partition count mismatch", mutate: func(m *Manifest) {}, count: 3, wantErr: "partition count"},
		{name: "wrong format version", mutate: func(m *Manifest) { m.FormatVersion = 99 }, count: 2, wantErr: "format version"},
		{name: "missing partition entry", mutate: func(m *Manifest) { delete(m.Partitions, 2) }, count: 2, wantErr: "missing partition"},
		{name: "empty checksum", mutate: func(m *Manifest) { p := m.Partitions[1]; p.SHA256 = ""; m.Partitions[1] = p }, count: 2, wantErr: "checksum"},
		{name: "schema newer than binary", mutate: func(m *Manifest) { p := m.Partitions[1]; p.SchemaVersion = "9999_future.up.sql"; m.Partitions[1] = p }, count: 2, wantErr: "schema"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := validManifest()
			tt.mutate(&m)
			err := m.Validate(tt.count, binSchema)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
