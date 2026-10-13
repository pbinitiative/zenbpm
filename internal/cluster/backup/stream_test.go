package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceivePartitionRestore(t *testing.T) {
	t.Run("loads the decompressed image and removes spool files", func(t *testing.T) {
		raw := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte("d"), 5000)...)
		gz := gzipBytes(t, raw)
		sum := sha256.Sum256(gz)
		meta := &proto.RestoreMeta{PartitionId: new(uint32(1)), Sha256: new(hex.EncodeToString(sum[:])), SizeBytes: new(int64(len(gz)))}

		dst := &fakeLoadTarget{}
		spoolDir := t.TempDir()
		err := ReceivePartitionRestore(context.Background(), spoolDir, meta, chunkFeed(meta, gz, 1024), RestoreLimits{}, dst.load)
		assert.NoError(t, err)
		assert.Equal(t, raw, dst.loaded, "the load hook sees the decompressed database on disk")
		assert.Equal(t, spoolDir, filepath.Dir(dst.path), "the decompressed database is spooled in the given directory, never in the OS temp dir")
		_, statErr := os.Stat(dst.path)
		assert.True(t, os.IsNotExist(statErr), "the decompressed spool is removed after the load")
		entries, err := os.ReadDir(spoolDir)
		require.NoError(t, err)
		assert.Empty(t, entries, "no spool file is left behind")
	})

	t.Run("rejects a mismatched digest", func(t *testing.T) {
		raw := append([]byte("SQLite format 3\x00"), []byte("data")...)
		gz := gzipBytes(t, raw)
		meta := &proto.RestoreMeta{PartitionId: new(uint32(1)), Sha256: new("deadbeef"), SizeBytes: new(int64(len(gz)))}
		dst := &fakeLoadTarget{}
		err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz, 1024), RestoreLimits{}, dst.load)
		assert.ErrorContains(t, err, "digest mismatch")
		assert.Nil(t, dst.loaded)
	})

	t.Run("rejects an image that is not SQLite", func(t *testing.T) {
		gz := gzipBytes(t, []byte("not a database at all"))
		sum := sha256.Sum256(gz)
		meta := &proto.RestoreMeta{PartitionId: new(uint32(1)), Sha256: new(hex.EncodeToString(sum[:])), SizeBytes: new(int64(len(gz)))}
		dst := &fakeLoadTarget{}
		err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz, 1024), RestoreLimits{}, dst.load)
		assert.ErrorContains(t, err, "not a valid SQLite")
		assert.Nil(t, dst.loaded)
	})

	t.Run("enforces image and database limits", func(t *testing.T) {
		raw := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte{0}, 4096)...)
		gz := gzipBytes(t, raw)
		meta := &proto.RestoreMeta{PartitionId: new(uint32(1)), Sha256: new(shaHex(gz)), SizeBytes: new(int64(len(gz)))}

		dst := &fakeLoadTarget{}
		err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz, 1024), RestoreLimits{PartitionImageBytes: int64(len(gz)) - 1}, dst.load)
		require.ErrorIs(t, err, zenerr.ErrResourceLimit)
		assert.Nil(t, dst.loaded, "an oversized image is never loaded")

		err = ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz, 1024), RestoreLimits{PartitionDatabaseBytes: 1024}, dst.load)
		require.ErrorIs(t, err, zenerr.ErrResourceLimit, "a compressible image must not expand past the database limit")
		assert.Nil(t, dst.loaded)

		err = ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz, 1024), RestoreLimits{PartitionImageBytes: int64(len(gz)), PartitionDatabaseBytes: int64(len(raw))}, dst.load)
		require.NoError(t, err)
		assert.Equal(t, raw, dst.loaded)
	})

	t.Run("stops on a cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		meta := &proto.RestoreMeta{PartitionId: new(uint32(1))}
		dst := &fakeLoadTarget{}
		err := ReceivePartitionRestore(ctx, t.TempDir(), meta, func() (*proto.RestoreChunk, error) {
			t.Fatal("recv must not be called once the context is done")
			return nil, nil
		}, RestoreLimits{}, dst.load)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// fakeLoadTarget records the decompressed database handed to the load hook.
// It reads the file while the hook runs: the spool must be complete by then
// and is removed afterwards.
type fakeLoadTarget struct {
	loaded []byte
	path   string
	err    error
}

func (f *fakeLoadTarget) load(ctx context.Context, databasePath string) error {
	data, err := os.ReadFile(databasePath)
	if err != nil {
		return err
	}
	f.loaded = data
	f.path = databasePath
	return f.err
}

func chunkFeed(meta *proto.RestoreMeta, data []byte, chunk int) func() (*proto.RestoreChunk, error) {
	sent, metaSent, eofSent := 0, false, false
	return func() (*proto.RestoreChunk, error) {
		if !metaSent {
			metaSent = true
			return &proto.RestoreChunk{Payload: &proto.RestoreChunk_Meta{Meta: meta}}, nil
		}
		if sent < len(data) {
			end := sent + chunk
			if end > len(data) {
				end = len(data)
			}
			c := &proto.RestoreChunk{Payload: &proto.RestoreChunk_Data{Data: data[sent:end]}}
			sent = end
			return c, nil
		}
		if !eofSent {
			eofSent = true
			return &proto.RestoreChunk{Eof: new(true)}, nil
		}
		return nil, io.EOF
	}
}
