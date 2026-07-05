package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/proto"
	"github.com/pbinitiative/zenbpm/pkg/ptr"
	rqcmd "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/stretchr/testify/assert"
)

type fakeBackupSource struct {
	payload []byte
	err     error
	gotReq  *rqcmd.BackupRequest
}

func (f *fakeBackupSource) Backup(ctx context.Context, br *rqcmd.BackupRequest, dst io.Writer) error {
	f.gotReq = br
	if f.err != nil {
		return f.err
	}
	_, err := dst.Write(f.payload)
	return err
}

func TestStreamPartitionBackup(t *testing.T) {
	payload := bytes.Repeat([]byte("zen"), 700_000) // > 1 chunk (1 MiB)
	src := &fakeBackupSource{payload: payload}
	var chunks []*proto.BackupChunk
	err := StreamPartitionBackup(context.Background(), src, "0007_x.up.sql", func(c *proto.BackupChunk) error {
		chunks = append(chunks, c)
		return nil
	})
	assert.NoError(t, err)

	// request must ask the leader for a vacuumed, compressed binary copy
	assert.True(t, src.gotReq.Leader)
	assert.True(t, src.gotReq.Vacuum)
	assert.True(t, src.gotReq.Compress)
	assert.Equal(t, rqcmd.BackupRequest_BACKUP_REQUEST_FORMAT_BINARY, src.gotReq.Format)

	// last chunk is the digest trailer
	last := chunks[len(chunks)-1]
	assert.True(t, last.GetEof())
	assert.Empty(t, last.GetData())
	assert.Equal(t, "0007_x.up.sql", last.GetSchemaVersion())
	sum := sha256.Sum256(payload)
	assert.Equal(t, hex.EncodeToString(sum[:]), last.GetSha256())

	// data chunks reassemble the payload and respect the chunk size
	var got []byte
	for _, c := range chunks[:len(chunks)-1] {
		assert.False(t, c.GetEof())
		assert.LessOrEqual(t, len(c.GetData()), backupChunkSize)
		got = append(got, c.GetData()...)
	}
	assert.Equal(t, payload, got)
	assert.Greater(t, len(chunks), 2)
}

func TestStreamPartitionBackupSourceError(t *testing.T) {
	src := &fakeBackupSource{err: errors.New("boom")}
	err := StreamPartitionBackup(context.Background(), src, "s", func(c *proto.BackupChunk) error { return nil })
	assert.ErrorContains(t, err, "boom")
}

type fakeLoadTarget struct {
	loaded []byte
	err    error
}

func (f *fakeLoadTarget) Load(ctx context.Context, lr *rqcmd.LoadRequest) error {
	f.loaded = lr.Data
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
			return &proto.RestoreChunk{Eof: ptr.To(true)}, nil
		}
		return nil, io.EOF
	}
}

func TestReceivePartitionRestore(t *testing.T) {
	raw := append([]byte("SQLite format 3\x00"), bytes.Repeat([]byte("d"), 5000)...)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(raw)
	zw.Close()
	sum := sha256.Sum256(gz.Bytes())
	meta := &proto.RestoreMeta{PartitionId: ptr.To(uint32(1)), Sha256: ptr.To(hex.EncodeToString(sum[:])), SizeBytes: ptr.To(int64(gz.Len()))}

	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.NoError(t, err)
	assert.Equal(t, raw, dst.loaded)
}

func TestReceivePartitionRestoreBadDigest(t *testing.T) {
	raw := append([]byte("SQLite format 3\x00"), []byte("data")...)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write(raw)
	zw.Close()
	meta := &proto.RestoreMeta{PartitionId: ptr.To(uint32(1)), Sha256: ptr.To("deadbeef"), SizeBytes: ptr.To(int64(gz.Len()))}
	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.ErrorContains(t, err, "digest mismatch")
	assert.Nil(t, dst.loaded)
}

func TestReceivePartitionRestoreNotSQLite(t *testing.T) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("not a database at all"))
	zw.Close()
	sum := sha256.Sum256(gz.Bytes())
	meta := &proto.RestoreMeta{PartitionId: ptr.To(uint32(1)), Sha256: ptr.To(hex.EncodeToString(sum[:])), SizeBytes: ptr.To(int64(gz.Len()))}
	dst := &fakeLoadTarget{}
	err := ReceivePartitionRestore(context.Background(), t.TempDir(), meta, chunkFeed(meta, gz.Bytes(), 1024), dst)
	assert.ErrorContains(t, err, "not a valid SQLite")
	assert.Nil(t, dst.loaded)
}
