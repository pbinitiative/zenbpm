package backup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
)

// backupChunkSize bounds each gRPC message well below default frame limits.
const backupChunkSize = 1 << 20 // 1 MiB

// BackupSource is the subset of *rqlite/store.Store used for backups.
type BackupSource interface {
	Backup(ctx context.Context, br *rqcmd.BackupRequest, dst io.Writer) error
}

// chunkSendWriter adapts a chunk-send callback into an io.Writer,
// hashing everything written so the digest is anchored at the source.
type chunkSendWriter struct {
	send func(*proto.BackupChunk) error
	hash hash.Hash
}

func (w *chunkSendWriter) Write(p []byte) (int, error) {
	w.hash.Write(p)
	for off := 0; off < len(p); off += backupChunkSize {
		end := off + backupChunkSize
		if end > len(p) {
			end = len(p)
		}
		// copy: gRPC may retain the buffer past Send
		data := append([]byte(nil), p[off:end]...)
		if err := w.send(&proto.BackupChunk{Data: data}); err != nil {
			return off, err
		}
	}
	return len(p), nil
}

// StreamPartitionBackup produces a vacuumed, gzipped, leader-consistent copy of
// the partition database as a chunk stream, terminated by an eof chunk that
// carries the sha256 of all sent bytes and the partition's schema version.
func StreamPartitionBackup(ctx context.Context, src BackupSource, schemaVersion string, send func(*proto.BackupChunk) error) error {
	w := &chunkSendWriter{send: send, hash: sha256.New()}
	br := &rqcmd.BackupRequest{
		Format:   rqcmd.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY,
		Leader:   true,
		Vacuum:   true,
		Compress: true,
	}
	if err := src.Backup(ctx, br, w); err != nil {
		return fmt.Errorf("partition backup failed: %w", err)
	}
	return send(&proto.BackupChunk{
		Eof:           new(true),
		Sha256:        new(hex.EncodeToString(w.hash.Sum(nil))),
		SchemaVersion: new(schemaVersion),
	})
}

// ReceivePartitionRestore spools the incoming gzipped stream, verifies its
// digest against meta, decompresses it into a second spool file, checks that
// the result is a SQLite database and hands its path to load. Nothing of the
// image is held in memory: the stored image and the decompressed database are
// capped by limits as disk usage. The caller copies the database into the
// partition (see CopyDatabase) under the partition's restore fence.
func ReceivePartitionRestore(ctx context.Context, spoolDir string, meta *proto.RestoreMeta, recv func() (*proto.RestoreChunk, error), limits RestoreLimits, load func(ctx context.Context, databasePath string) error) (err error) {
	limits = limits.withDefaults()
	spool, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-recv-p%d-*", meta.GetPartitionId()))
	if err != nil {
		return fmt.Errorf("failed to create restore spool: %w", err)
	}
	defer func() {
		// runs after the close below; a leftover spool file must not fail a
		// restore that already loaded
		if removeErr := os.Remove(spool.Name()); removeErr != nil {
			log.Warn("failed to remove restore spool file %s: %v", spool.Name(), removeErr)
		}
	}()
	defer zenerr.CloseJoin(spool, &err, "restore spool file")

	h := sha256.New()
	w := io.MultiWriter(spool, h)
	var received int64
recvLoop:
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("restore stream cancelled: %w", err)
		}
		chunk, err := recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("restore stream failed: %w", err)
		}
		switch p := chunk.GetPayload().(type) {
		case *proto.RestoreChunk_Meta:
			// tolerated duplicate of the header chunk; ignore
			_ = p
		case *proto.RestoreChunk_Data:
			received += int64(len(p.Data))
			if received > limits.PartitionImageBytes {
				return fmt.Errorf("%w: partition image exceeds %d bytes", zenerr.ErrResourceLimit, limits.PartitionImageBytes)
			}
			if _, err := w.Write(p.Data); err != nil {
				return fmt.Errorf("failed to spool restore data: %w", err)
			}
		}
		if chunk.GetEof() {
			break recvLoop
		}
	}

	got := hex.EncodeToString(h.Sum(nil))
	if got != meta.GetSha256() {
		return fmt.Errorf("digest mismatch for partition %d: expected %s, received %s", meta.GetPartitionId(), meta.GetSha256(), got)
	}

	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return err
	}
	zr, err := gzip.NewReader(spool)
	if err != nil {
		return fmt.Errorf("restore payload is not gzip: %w", err)
	}
	head := make([]byte, len(sqliteHeader))
	if _, err := io.ReadFull(zr, head); err != nil || string(head) != sqliteHeader {
		return fmt.Errorf("restore payload is not a valid SQLite database")
	}
	// The database is decompressed to disk, never into memory: the copy into
	// the partition reads it row by row, so the restorable size is bounded by
	// the configured limit and disk space, not by RAM.
	database, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-db-p%d-*", meta.GetPartitionId()))
	if err != nil {
		return fmt.Errorf("failed to create restore database spool: %w", err)
	}
	defer func() {
		if removeErr := os.Remove(database.Name()); removeErr != nil {
			log.Warn("failed to remove restore database spool file %s: %v", database.Name(), removeErr)
		}
	}()
	if _, err := database.Write(head); err != nil {
		_ = database.Close()
		return fmt.Errorf("failed to write restore database spool: %w", err)
	}
	_, err = copyAtMost(database, &contextReader{ctx: ctx, r: zr}, limits.PartitionDatabaseBytes-int64(len(head)), "decompressed database")
	if closeErr := database.Close(); closeErr != nil && err == nil {
		err = fmt.Errorf("failed to close restore database spool: %w", closeErr)
	}
	if err != nil {
		if errors.Is(err, zenerr.ErrResourceLimit) || ctx.Err() != nil {
			return err
		}
		return fmt.Errorf("failed to decompress restore payload: %w", err)
	}
	if err := load(ctx, database.Name()); err != nil {
		return fmt.Errorf("failed to load database into partition %d: %w", meta.GetPartitionId(), err)
	}
	return nil
}

// sqliteHeader is the magic string every SQLite database file starts with.
const sqliteHeader = "SQLite format 3\x00"
