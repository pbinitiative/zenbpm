package backup

import (
	"archive/tar"
	"compress/gzip"
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

// Bundle holds a validated, spooled restore bundle ready for use.
// The Manifest field is populated after OpenBundle succeeds.
// Call Close when done to remove the spool files.
type Bundle struct {
	Manifest Manifest
	files    map[uint32]string // partition id -> spooled gz file path
}

// OpenBundle spools a bundle stream to disk and fully validates it BEFORE any
// destructive restore step: manifest present, every partition file present
// with matching sha256 and size, and gunzipped content that looks like SQLite.
func OpenBundle(r io.Reader, spoolDir string) (*Bundle, error) {
	b := &Bundle{files: map[uint32]string{}}
	tr := tar.NewReader(r)
	shas := map[uint32]string{}
	sizes := map[uint32]int64{}
	manifestSeen := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to read bundle (truncated or corrupt tar): %w", err)
		}
		var id uint32
		if hdr.Name == ManifestFileName {
			if err := json.NewDecoder(tr).Decode(&b.Manifest); err != nil {
				b.Close()
				return nil, fmt.Errorf("failed to parse manifest: %w", err)
			}
			manifestSeen = true
			continue
		}
		if _, err := fmt.Sscanf(hdr.Name, "partition-%d.db.gz", &id); err != nil {
			b.Close()
			return nil, fmt.Errorf("unexpected bundle entry %q", hdr.Name)
		}
		f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-p%d-*", id))
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to create restore spool: %w", err)
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(f, h), tr)
		f.Close()
		if err != nil {
			b.Close()
			return nil, fmt.Errorf("failed to spool %s: %w", hdr.Name, err)
		}
		b.files[id] = f.Name()
		shas[id] = hex.EncodeToString(h.Sum(nil))
		sizes[id] = n
	}
	if !manifestSeen {
		b.Close()
		return nil, fmt.Errorf("bundle has no %s (incomplete backup?)", ManifestFileName)
	}
	for id, meta := range b.Manifest.Partitions {
		if _, ok := b.files[id]; !ok {
			b.Close()
			return nil, fmt.Errorf("bundle is missing file for partition %d", id)
		}
		if shas[id] != meta.SHA256 {
			b.Close()
			return nil, fmt.Errorf("checksum mismatch for partition %d: manifest %s, bundle %s", id, meta.SHA256, shas[id])
		}
		if sizes[id] != meta.SizeBytes {
			b.Close()
			return nil, fmt.Errorf("size mismatch for partition %d", id)
		}
		if err := verifySQLiteGzip(b.files[id]); err != nil {
			b.Close()
			return nil, fmt.Errorf("partition %d: %w", id, err)
		}
	}
	for id := range b.files {
		if _, ok := b.Manifest.Partitions[id]; !ok {
			b.Close()
			return nil, fmt.Errorf("bundle contains partition %d not listed in manifest", id)
		}
	}
	return b, nil
}

// verifySQLiteGzip opens the gzip file at path, checks the SQLite magic header,
// and drains the stream so gzip verifies its CRC over the whole content.
func verifySQLiteGzip(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("not gzip data: %w", err)
	}
	defer zr.Close()
	head := make([]byte, 16)
	if _, err := io.ReadFull(zr, head); err != nil {
		return fmt.Errorf("failed to read database header: %w", err)
	}
	if string(head) != "SQLite format 3\x00" {
		return fmt.Errorf("content is not a valid SQLite database")
	}
	// drain to let gzip verify its CRC over the whole stream
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return fmt.Errorf("gzip stream corrupt: %w", err)
	}
	return nil
}

// PartitionFile returns a ReadCloser over the stored (still-gzipped) bytes for
// the given partition id. The caller must close the returned ReadCloser.
func (b *Bundle) PartitionFile(id uint32) (io.ReadCloser, error) {
	path, ok := b.files[id]
	if !ok {
		return nil, fmt.Errorf("no file for partition %d", id)
	}
	return os.Open(path)
}

// Close removes all spooled partition files.
func (b *Bundle) Close() error {
	for _, p := range b.files {
		os.Remove(p)
	}
	return nil
}
