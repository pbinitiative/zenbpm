package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/config"
)

// RestoreLimits bounds the inputs of a restore. Zero values take the defaults.
type RestoreLimits struct {
	// ManifestBytes caps the bundle manifest.
	ManifestBytes int64
	// PartitionImageBytes caps one stored (gzipped) partition image.
	PartitionImageBytes int64
	// PartitionDatabaseBytes caps one decompressed partition database.
	PartitionDatabaseBytes int64
}

// DefaultRestoreLimits returns the limits used when a field is zero.
func DefaultRestoreLimits() RestoreLimits {
	return RestoreLimits{
		ManifestBytes:          1 << 20,
		PartitionImageBytes:    8 << 30,
		PartitionDatabaseBytes: 16 << 30,
	}
}

// RestoreLimitsFromConfig maps the cluster configuration onto RestoreLimits.
func RestoreLimitsFromConfig(c config.Restore) RestoreLimits {
	return RestoreLimits{
		ManifestBytes:          c.MaxManifestBytes,
		PartitionImageBytes:    c.MaxPartitionImageBytes,
		PartitionDatabaseBytes: c.MaxPartitionDatabaseBytes,
	}
}

func (l RestoreLimits) withDefaults() RestoreLimits {
	def := DefaultRestoreLimits()
	pick := func(v, d int64) int64 {
		if v <= 0 {
			return d
		}
		return v
	}
	return RestoreLimits{
		ManifestBytes:          pick(l.ManifestBytes, def.ManifestBytes),
		PartitionImageBytes:    pick(l.PartitionImageBytes, def.PartitionImageBytes),
		PartitionDatabaseBytes: pick(l.PartitionDatabaseBytes, def.PartitionDatabaseBytes),
	}
}

// copyAtMost copies r into w and fails with ErrResourceLimit once more than
// limit bytes were read, without buffering the input.
func copyAtMost(w io.Writer, r io.Reader, limit int64, what string) (int64, error) {
	n, err := io.Copy(w, io.LimitReader(r, limit+1))
	if err != nil {
		return n, err
	}
	if n > limit {
		return n, fmt.Errorf("%w: %s exceeds %d bytes", zenerr.ErrResourceLimit, what, limit)
	}
	return n, nil
}

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
				// best-effort cleanup; a leftover spool file is harmless
				_ = os.Remove(r.path)
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
		PartitionCount:  uint32(len(partitionIDs)), // #nosec G115 -- partition counts are far below MaxUint32
		Partitions:      make(map[uint32]PartitionMeta, len(partitionIDs)),
	}

	tw := tar.NewWriter(w)
	// Write partition entries in ascending ID order.
	for _, id := range partitionIDs {
		r := results[id]
		if err := writeSpoolEntry(tw, PartitionFileName(id), r.path, r.size); err != nil {
			return nil, err
		}
		// best-effort cleanup; a leftover spool file is harmless
		_ = os.Remove(r.path)
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
func spoolPartition(ctx context.Context, spoolDir string, id uint32, fetch FetchFunc) (out spoolResult) {
	f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-backup-p%d-*", id))
	if err != nil {
		return spoolResult{err: fmt.Errorf("failed to create spool file: %w", err)}
	}
	defer func() {
		closeErr := f.Close()
		if closeErr == nil {
			return
		}
		// the spool file is re-read when the bundle is assembled, so a spool
		// that did not close cleanly must not be reported as a success
		if out.err == nil {
			_ = os.Remove(f.Name()) // best-effort cleanup on the error path
			out = spoolResult{}
		}
		out.err = errors.Join(out.err, fmt.Errorf("failed to close spool file for partition %d: %w", id, closeErr))
	}()
	snapshotAt := time.Now().UnixMilli()
	h := sha256.New()
	res, err := fetch(ctx, id, io.MultiWriter(f, h))
	if err != nil {
		_ = os.Remove(f.Name()) // best-effort cleanup on the error path
		return spoolResult{err: err}
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != res.SHA256 {
		_ = os.Remove(f.Name()) // best-effort cleanup on the error path
		return spoolResult{err: fmt.Errorf("digest mismatch for partition %d: source declared %s, coordinator computed %s", id, res.SHA256, got)}
	}
	info, err := f.Stat()
	if err != nil {
		_ = os.Remove(f.Name()) // best-effort cleanup on the error path
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
func writeSpoolEntry(tw *tar.Writer, name, path string, size int64) (err error) {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: size}); err != nil {
		return fmt.Errorf("failed to write tar header for %s: %w", name, err)
	}
	f, err := os.Open(path) // #nosec G304 -- path is a spool file this process created via os.CreateTemp
	if err != nil {
		return fmt.Errorf("failed to reopen spool %s: %w", filepath.Base(path), err)
	}
	defer zenerr.CloseJoin(f, &err, "spool file "+filepath.Base(path))
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
// destructive restore step: only regular entries with well-formed names, no
// duplicate partition or manifest entries, no partition id outside
// 1..expectedPartitions (0 disables that check), manifest present exactly
// once and free of duplicate members, every partition file present with
// matching sha256 and size, sizes within limits, and gunzipped content that
// looks like SQLite. Spool files of a rejected bundle are removed.
func OpenBundle(ctx context.Context, r io.Reader, spoolDir string, expectedPartitions uint32, limits RestoreLimits) (*Bundle, error) {
	limits = limits.withDefaults()
	b := &Bundle{files: map[uint32]string{}}
	fail := func(err error) (*Bundle, error) {
		_ = b.Close()
		return nil, err
	}
	// every read of the upload observes ctx; verification of the spooled
	// files below does the same, so the whole ingest is bounded by one deadline
	tr := tar.NewReader(&contextReader{ctx: ctx, r: r})
	shas := map[uint32]string{}
	sizes := map[uint32]int64{}
	manifestSeen := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fail(fmt.Errorf("failed to read bundle (truncated or corrupt tar): %w", err))
		}
		if hdr.Typeflag != tar.TypeReg {
			return fail(fmt.Errorf("unexpected bundle entry %q: not a regular file (type %q)", hdr.Name, hdr.Typeflag))
		}
		if hdr.Name == ManifestFileName {
			if manifestSeen {
				return fail(fmt.Errorf("bundle contains more than one %s", ManifestFileName))
			}
			manifest, err := DecodeManifest(tr, limits.ManifestBytes)
			if err != nil {
				return fail(err)
			}
			b.Manifest = manifest
			manifestSeen = true
			continue
		}
		id, ok := ParsePartitionFileName(hdr.Name)
		if !ok {
			return fail(fmt.Errorf("unexpected bundle entry %q", hdr.Name))
		}
		if expectedPartitions > 0 && id > expectedPartitions {
			return fail(fmt.Errorf("bundle entry %q: partition %d is outside the cluster's partitions 1..%d", hdr.Name, id, expectedPartitions))
		}
		if _, dup := b.files[id]; dup {
			return fail(fmt.Errorf("bundle contains partition %d more than once", id))
		}
		f, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-p%d-*", id))
		if err != nil {
			return fail(fmt.Errorf("failed to create restore spool: %w", err))
		}
		// register the spool file first so every error path below removes it
		b.files[id] = f.Name()
		h := sha256.New()
		// The stream is an operator-supplied backup of whole partition databases,
		// spooled to disk (not memory), capped by the configured image limit and
		// size-checked against the manifest below.
		n, err := copyAtMost(io.MultiWriter(f, h), tr, limits.PartitionImageBytes, hdr.Name)
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		if err != nil {
			return fail(fmt.Errorf("failed to spool %s: %w", hdr.Name, err))
		}
		shas[id] = hex.EncodeToString(h.Sum(nil))
		sizes[id] = n
	}
	if !manifestSeen {
		return fail(fmt.Errorf("bundle has no %s (incomplete backup?)", ManifestFileName))
	}
	for id, meta := range b.Manifest.Partitions {
		if _, ok := b.files[id]; !ok {
			return fail(fmt.Errorf("bundle is missing file for partition %d", id))
		}
		if shas[id] != meta.SHA256 {
			return fail(fmt.Errorf("checksum mismatch for partition %d: manifest %s, bundle %s", id, meta.SHA256, shas[id]))
		}
		if sizes[id] != meta.SizeBytes {
			return fail(fmt.Errorf("size mismatch for partition %d", id))
		}
		if err := verifySQLiteGzip(ctx, b.files[id], limits.PartitionDatabaseBytes); err != nil {
			return fail(fmt.Errorf("partition %d: %w", id, err))
		}
	}
	for id := range b.files {
		if _, ok := b.Manifest.Partitions[id]; !ok {
			return fail(fmt.Errorf("bundle contains partition %d not listed in manifest", id))
		}
	}
	return b, nil
}

// verifySQLiteGzip opens the gzip file at path, checks the SQLite magic header,
// and drains the stream so gzip verifies its CRC over the whole content. The
// decompressed size is capped by maxDatabaseBytes so a highly compressible
// image cannot expand without bound.
func verifySQLiteGzip(ctx context.Context, path string, maxDatabaseBytes int64) (err error) {
	f, err := os.Open(path) // #nosec G304 -- path is a spool file this process created via os.CreateTemp
	if err != nil {
		return err
	}
	defer zenerr.CloseJoin(f, &err, "spool file "+filepath.Base(path))
	// decompressing a multi-gigabyte image takes a while; the drain below
	// stops at the ingest deadline instead of running to the end regardless
	zr, err := gzip.NewReader(&contextReader{ctx: ctx, r: f})
	if err != nil {
		return fmt.Errorf("not gzip data: %w", err)
	}
	defer zenerr.CloseJoin(zr, &err, "gzip reader")
	head := make([]byte, 16)
	if _, err := io.ReadFull(zr, head); err != nil {
		return fmt.Errorf("failed to read database header: %w", err)
	}
	if string(head) != "SQLite format 3\x00" {
		return fmt.Errorf("content is not a valid SQLite database")
	}
	// Drain to let gzip verify its CRC over the whole stream. Output is
	// discarded, so memory use stays constant, and the decompressed size is
	// capped.
	if _, err := copyAtMost(io.Discard, zr, maxDatabaseBytes-int64(len(head)), "decompressed database"); err != nil {
		if errors.Is(err, zenerr.ErrResourceLimit) || ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("gzip stream corrupt: %w", err)
	}
	return nil
}

// contextReader fails reads once ctx is done, so neither a stalled upload nor
// a long verification can run past the ingest deadline.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *contextReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// PartitionFile returns a ReadCloser over the stored (still-gzipped) bytes for
// the given partition id. The caller must close the returned ReadCloser.
func (b *Bundle) PartitionFile(id uint32) (io.ReadCloser, error) {
	path, ok := b.files[id]
	if !ok {
		return nil, fmt.Errorf("no file for partition %d", id)
	}
	return os.Open(path) // #nosec G304 -- path is a spool file this process created via os.CreateTemp
}

// Close removes all spooled partition files.
func (b *Bundle) Close() error {
	var err error
	for _, p := range b.files {
		if removeErr := os.Remove(p); removeErr != nil && !os.IsNotExist(removeErr) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}
