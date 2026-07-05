package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func testFetch(payloads map[uint32][]byte) FetchFunc {
	return func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		p, ok := payloads[id]
		if !ok {
			return FetchResult{}, fmt.Errorf("no payload for %d", id)
		}
		if _, err := dst.Write(p); err != nil {
			return FetchResult{}, err
		}
		return FetchResult{SHA256: shaHex(p), SchemaVersion: "0007_x.up.sql"}, nil
	}
}

func TestWriteBundleRoundTrip(t *testing.T) {
	payloads := map[uint32][]byte{1: []byte("partition-one-data"), 2: []byte("partition-two-data")}
	var buf bytes.Buffer
	m, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, testFetch(payloads))
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), m.PartitionCount)

	tr := tar.NewReader(&buf)
	var names []string
	files := map[string][]byte{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		b, _ := io.ReadAll(tr)
		names = append(names, hdr.Name)
		files[hdr.Name] = b
	}
	// partition files in ascending order, manifest last
	assert.Equal(t, []string{"partition-1.db.gz", "partition-2.db.gz", "manifest.json"}, names)
	assert.Equal(t, payloads[1], files["partition-1.db.gz"])
	assert.Equal(t, payloads[2], files["partition-2.db.gz"])

	var parsed Manifest
	assert.NoError(t, json.Unmarshal(files["manifest.json"], &parsed))
	assert.Equal(t, shaHex(payloads[1]), parsed.Partitions[1].SHA256)
	assert.Equal(t, int64(len(payloads[2])), parsed.Partitions[2].SizeBytes)
	assert.Equal(t, "0007_x.up.sql", parsed.Partitions[1].SchemaVersion)
}

func TestWriteBundleDigestMismatchAborts(t *testing.T) {
	lying := func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		dst.Write([]byte("actual bytes"))
		return FetchResult{SHA256: "deadbeef", SchemaVersion: "s"}, nil
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, lying)
	assert.ErrorContains(t, err, "digest mismatch")
}

func TestWriteBundleFetchErrorAborts(t *testing.T) {
	failing := func(ctx context.Context, id uint32, dst io.Writer) (FetchResult, error) {
		return FetchResult{}, errors.New("leader unreachable")
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, failing)
	assert.ErrorContains(t, err, "leader unreachable")
}
