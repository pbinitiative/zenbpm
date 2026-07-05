package backup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
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

// LoadTarget is the subset of *rqlite/store.Store used for restores.
type LoadTarget interface {
	Load(ctx context.Context, lr *rqcmd.LoadRequest) error
}

// ReceivePartitionRestore spools the incoming gzipped stream, verifies its
// digest against meta, gunzips and validates the SQLite image, then loads it
// through the partition's raft log.
func ReceivePartitionRestore(ctx context.Context, spoolDir string, meta *proto.RestoreMeta, recv func() (*proto.RestoreChunk, error), dst LoadTarget) error {
	spool, err := os.CreateTemp(spoolDir, fmt.Sprintf("zenbpm-restore-recv-p%d-*", meta.GetPartitionId()))
	if err != nil {
		return fmt.Errorf("failed to create restore spool: %w", err)
	}
	defer os.Remove(spool.Name())
	defer spool.Close()

	h := sha256.New()
	w := io.MultiWriter(spool, h)
recvLoop:
	for {
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
	raw, err := io.ReadAll(zr)
	if err != nil {
		return fmt.Errorf("failed to decompress restore payload: %w", err)
	}
	if len(raw) < 16 || string(raw[:16]) != "SQLite format 3\x00" {
		return fmt.Errorf("restore payload is not a valid SQLite database")
	}
	if err := dst.Load(ctx, &rqcmd.LoadRequest{Data: raw}); err != nil {
		return fmt.Errorf("failed to load database into partition %d: %w", meta.GetPartitionId(), err)
	}
	return nil
}
