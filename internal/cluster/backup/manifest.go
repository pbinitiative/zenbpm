// Package backup implements cluster-wide backup and restore of partition
// databases as a streamed tar bundle with a trailing manifest.
package backup

import (
	"fmt"
	"runtime/debug"
	"strings"
)

const (
	ManifestFormatVersion = 1
	ManifestFileName      = "manifest.json"
)

type PartitionMeta struct {
	SnapshotAtMillis int64  `json:"snapshotAtMillis"`
	SizeBytes        int64  `json:"sizeBytes"`
	// SHA256 is the hex digest of the stored (gzipped) partition file bytes,
	// computed on the partition leader while streaming.
	SHA256        string `json:"sha256"`
	SchemaVersion string `json:"schemaVersion"`
}

type Manifest struct {
	FormatVersion   int                      `json:"formatVersion"`
	ZenBPMVersion   string                   `json:"zenbpmVersion"`
	CreatedAtMillis int64                    `json:"createdAtMillis"`
	PartitionCount  uint32                   `json:"partitionCount"`
	Partitions      map[uint32]PartitionMeta `json:"partitions"`
}

func PartitionFileName(id uint32) string {
	return fmt.Sprintf("partition-%d.db.gz", id)
}

// Validate checks that the manifest can be restored into a cluster with
// clusterPartitionCount partitions running a binary whose newest migration
// is binarySchemaVersion. Migration filenames sort lexically (0001_, 0002_, ...).
func (m *Manifest) Validate(clusterPartitionCount uint32, binarySchemaVersion string) error {
	if m.FormatVersion != ManifestFormatVersion {
		return fmt.Errorf("unsupported manifest format version %d (supported: %d)", m.FormatVersion, ManifestFormatVersion)
	}
	if m.PartitionCount != clusterPartitionCount {
		return fmt.Errorf("backup partition count %d does not match cluster partition count %d", m.PartitionCount, clusterPartitionCount)
	}
	for id := uint32(1); id <= m.PartitionCount; id++ {
		meta, ok := m.Partitions[id]
		if !ok {
			return fmt.Errorf("manifest is missing partition %d", id)
		}
		if meta.SHA256 == "" {
			return fmt.Errorf("manifest checksum for partition %d is empty", id)
		}
		if strings.Compare(meta.SchemaVersion, binarySchemaVersion) > 0 {
			return fmt.Errorf("partition %d schema version %q is newer than this binary's %q", id, meta.SchemaVersion, binarySchemaVersion)
		}
	}
	return nil
}

// ZenBPMVersion returns the running module version for the manifest.
func ZenBPMVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "unknown"
}
