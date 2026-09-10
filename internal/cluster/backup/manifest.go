// Package backup implements cluster-wide backup and restore of partition
// databases as a streamed tar bundle with a trailing manifest.
package backup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	zensql "github.com/pbinitiative/zenbpm/internal/sql"
)

const (
	ManifestFormatVersion = 1
	ManifestFileName      = "manifest.json"
)

type PartitionMeta struct {
	SnapshotAtMillis int64 `json:"snapshotAtMillis"`
	SizeBytes        int64 `json:"sizeBytes"`
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

// partitionFileNamePattern is the exact shape PartitionFileName produces; a
// leading zero, a sign, a directory component or trailing characters are all
// rejected.
var partitionFileNamePattern = regexp.MustCompile(`^partition-([1-9][0-9]{0,9})\.db\.gz$`)

// ParsePartitionFileName returns the partition id encoded in a bundle entry
// name, or false when the name is not a well-formed partition file name.
func ParsePartitionFileName(name string) (uint32, bool) {
	m := partitionFileNamePattern.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	id, err := strconv.ParseUint(m[1], 10, 32)
	if err != nil || id == 0 {
		return 0, false
	}
	return uint32(id), true
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
	for id := range m.Partitions {
		if id == 0 || id > m.PartitionCount {
			return fmt.Errorf("manifest lists partition %d which is outside the expected partitions 1..%d", id, m.PartitionCount)
		}
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

// DecodeManifest reads and validates the manifest JSON: at most maxBytes,
// exactly one JSON document, and no duplicate object members. Duplicate
// partition keys are detected on their numeric value, so "1" and "01" count as
// the same partition. encoding/json would otherwise silently keep the last of
// duplicate members, which lets an ambiguous manifest pass validation.
func DecodeManifest(r io.Reader, maxBytes int64) (Manifest, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultRestoreLimits().ManifestBytes
	}
	raw, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return Manifest{}, fmt.Errorf("failed to read manifest: %w", err)
	}
	if int64(len(raw)) > maxBytes {
		return Manifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", zenerr.ErrResourceLimit, maxBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := checkNoDuplicateMembers(dec, ""); err != nil {
		return Manifest{}, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("manifest contains data after the JSON document")
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("failed to parse manifest: %w", err)
	}
	return m, nil
}

// checkNoDuplicateMembers walks one JSON value token by token and rejects
// objects with repeated members. Members of the "partitions" object are
// compared by their numeric value.
func checkNoDuplicateMembers(dec *json.Decoder, path string) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("failed to parse manifest: %w", err)
	}
	switch delim := tok.(type) {
	case json.Delim:
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return fmt.Errorf("failed to parse manifest: %w", err)
				}
				key, _ := keyTok.(string)
				normalized := key
				if path == "partitions" {
					if id, err := strconv.ParseUint(key, 10, 32); err == nil {
						normalized = strconv.FormatUint(id, 10)
					}
				}
				if seen[normalized] {
					return fmt.Errorf("manifest member %q appears more than once", joinPath(path, key))
				}
				seen[normalized] = true
				if err := checkNoDuplicateMembers(dec, joinPath(path, key)); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing brace
				return fmt.Errorf("failed to parse manifest: %w", err)
			}
		case '[':
			for dec.More() {
				if err := checkNoDuplicateMembers(dec, path+"[]"); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil { // closing bracket
				return fmt.Errorf("failed to parse manifest: %w", err)
			}
		}
	}
	return nil
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// ZenBPMVersion returns the running module version for the manifest.
func ZenBPMVersion() string {
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" {
		return bi.Main.Version
	}
	return "unknown"
}

// BinarySchemaVersion returns the newest migration filename shipped with this
// binary, used to reject bundles created by a newer schema.
func BinarySchemaVersion(migrationDir string) (string, error) {
	migs, err := zensql.GetUpMigrations(migrationDir)
	if err != nil {
		return "", fmt.Errorf("failed to read migrations: %w", err)
	}
	var latest string
	for _, m := range migs {
		if m.Filename > latest {
			latest = m.Filename
		}
	}
	return latest, nil
}
