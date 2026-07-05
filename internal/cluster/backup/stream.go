package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
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
		Eof:           ptr.To(true),
		Sha256:        ptr.To(hex.EncodeToString(w.hash.Sum(nil))),
		SchemaVersion: ptr.To(schemaVersion),
	})
}
