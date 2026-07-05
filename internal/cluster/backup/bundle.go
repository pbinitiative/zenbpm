package backup

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FetchResult carries the source-declared digest and schema version returned
// by a FetchFunc after streaming a partition's backup data.
type FetchResult struct {
	SHA256        string
	SchemaVersion string
}

// FetchFunc streams one partition's backup into dst and returns the
// source-declared digest and schema version.
type FetchFunc func(ctx context.Context, partitionID uint32, dst io.Writer) (FetchResult, error)

// spoolResult holds the outcome of spooling a single partition to disk.
type spoolResult struct {
	path string
	size int64
	meta PartitionMeta
	err  error
}

// WriteBundle fans out to all partitions concurrently (bounding snapshot skew
// to seconds), spools each stream to disk while hashing it, verifies the
// coordinator-side digest against the source-declared one, then writes a plain
// tar: partition files in ascending id order, manifest.json last.
//
// partitionIDs must be pre-sorted ascending.
//
// Spool files are deleted after their tar entry is written (on the happy path)
// or on any error path via the deferred cleanup sweep.
func WriteBundle(ctx context.Context, w io.Writer, spoolDir string, partitionIDs []uint32, fetch FetchFunc) (*Manifest, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Fan-out: start all fetches concurrently.
	var wg sync.WaitGroup
	// results channel collects spool outcomes.
	type idResult struct {
		id uint32
		r  spoolResult
	}
	ch := make(chan idResult, len(partitionIDs))

	for _, id := range partitionIDs {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			ch <- idResult{id: id, r: spoolPartition(ctx, spoolDir, id, fetch)}
		}(id)
	}

	// Collect all results.
	go func() {
		wg.Wait()
		close(ch)
	}()

	results := make(map[uint32]spoolResult, len(partitionIDs))
	for item := range ch {
		results[item.id] = item.r
		if item.r.err != nil {
			cancel() // abort remaining in-flight fetches; bundle is already doomed
		}
	}

	// Deferred cleanup: remove any spool files not yet removed by the happy path.
	defer func() {
		for _, r := range results {
			if r.path != "" {
				os.Remove(r.path)
			}
		}
	}()

	// Check for any fetch/spool errors before touching the tar writer.
	for _, id := range partitionIDs {
		if r := results[id]; r.err != nil {
			return nil, fmt.Errorf("backup of partition %d failed: %w", id, r.err)
		}
	}

	manifest := &Manifest{
		FormatVersion:   ManifestFormatVersion,
		ZenBPMVersion:   ZenBPMVersion(),
		CreatedAtMillis: time.Now().UnixMilli(),
		PartitionCount:  uint32(len(partitionIDs)),
		Partitions:      make(map[uint32]PartitionMeta, len(partitionIDs)),
	}

	tw := tar.NewWriter(w)
	// Write partition entries in ascending ID order.
	for _, id := range partitionIDs {
		r := results[id]
		if err := writeSpoolEntry(tw, PartitionFileName(id), r.path, r.size); err != nil {
			return nil, err
		}
		os.Remove(r.path)
		// Mark as consumed so deferred cleanup skips it.
		r.path = ""
		results[id] = r
		manifest.Partitions[id] = r.meta
	}

	// Write manifest.json as the last tar entry.
	mb, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: ManifestFileName, Mode: 0o600, Size: int64(len(mb))}); err != nil {
		return nil, fmt.Errorf("failed to write manifest header: %w", err)
	}
	if _, err := tw.Write(mb); err != nil {
		return nil, fmt.Errorf("failed to write manifest: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize bundle: %w", err)
	}
	return manifest, nil
}

// spoolPartition streams a partition backup into a temporary file in spoolDir,
// hashing simultaneously. It verifies the coordinator-computed digest against
// the source-declared one and returns the file path and metadata on success.
func spoolPartition(ctx context.Context, spoolDir string, id uint32, fetch FetchFunc) spoolResult {
	f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-backup-p%d-*", id))
	if err != nil {
		return spoolResult{err: fmt.Errorf("failed to create spool file: %w", err)}
	}
	defer f.Close()
	snapshotAt := time.Now().UnixMilli()
	h := sha256.New()
	res, err := fetch(ctx, id, io.MultiWriter(f, h))
	if err != nil {
		os.Remove(f.Name())
		return spoolResult{err: err}
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != res.SHA256 {
		os.Remove(f.Name())
		return spoolResult{err: fmt.Errorf("digest mismatch for partition %d: source declared %s, coordinator computed %s", id, res.SHA256, got)}
	}
	info, err := f.Stat()
	if err != nil {
		os.Remove(f.Name())
		return spoolResult{err: err}
	}
	return spoolResult{
		path: f.Name(),
		size: info.Size(),
		meta: PartitionMeta{
			SnapshotAtMillis: snapshotAt,
			SizeBytes:        info.Size(),
			SHA256:           res.SHA256,
			SchemaVersion:    res.SchemaVersion,
		},
	}
}

// writeSpoolEntry copies the spool file at path into the tar archive as name.
func writeSpoolEntry(tw *tar.Writer, name, path string, size int64) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: size}); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", name, err)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to reopen spool %s: %w", filepath.Base(path), err)
	}
	defer f.Close()
	if _, err := io.Copy(tw, f); err != nil {
		return fmt.Errorf("failed to copy %s into bundle: %w", name, err)
	}
	return nil
}
