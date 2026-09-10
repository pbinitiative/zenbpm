package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		if _, err := dst.Write([]byte("actual bytes")); err != nil {
			return FetchResult{}, err
		}
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

func gzipBytes(t *testing.T, raw []byte) []byte {
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	_, err := zw.Write(raw)
	assert.NoError(t, err)
	assert.NoError(t, zw.Close())
	return b.Bytes()
}

func sqliteish(t *testing.T, tail string) []byte {
	return append([]byte("SQLite format 3\x00"), []byte(tail)...)
}

func TestOpenBundleRoundTrip(t *testing.T) {
	payloads := map[uint32][]byte{
		1: gzipBytes(t, sqliteish(t, "one")),
		2: gzipBytes(t, sqliteish(t, "two")),
	}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1, 2}, testFetch(payloads))
	assert.NoError(t, err)

	b, err := OpenBundle(context.Background(), &buf, t.TempDir(), 0, RestoreLimits{})
	assert.NoError(t, err)
	defer func() { require.NoError(t, b.Close()) }()
	assert.Equal(t, uint32(2), b.Manifest.PartitionCount)

	rc, err := b.PartitionFile(2)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, payloads[2], got)
}

func TestOpenBundleTruncated(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, sqliteish(t, "one"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	trunc := buf.Bytes()[:buf.Len()-600] // cut into/before the manifest entry
	_, err = OpenBundle(context.Background(), bytes.NewReader(trunc), t.TempDir(), 0, RestoreLimits{})
	assert.Error(t, err)
}

func TestOpenBundleCorruptedPartitionFile(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, sqliteish(t, "one"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	raw := buf.Bytes()
	// flip a byte inside the partition file body (first entry data starts at 512)
	raw[520] ^= 0xFF
	_, err = OpenBundle(context.Background(), bytes.NewReader(raw), t.TempDir(), 0, RestoreLimits{})
	assert.ErrorContains(t, err, "checksum")
}

func TestOpenBundleNotSQLite(t *testing.T) {
	payloads := map[uint32][]byte{1: gzipBytes(t, []byte("definitely not a database"))}
	var buf bytes.Buffer
	_, err := WriteBundle(context.Background(), &buf, t.TempDir(), []uint32{1}, testFetch(payloads))
	assert.NoError(t, err)
	_, err = OpenBundle(context.Background(), bytes.NewReader(buf.Bytes()), t.TempDir(), 0, RestoreLimits{})
	assert.ErrorContains(t, err, "not a valid SQLite")
}

// writeRawBundle builds a tar with arbitrary entries so malformed bundles can
// be fed to OpenBundle.
func writeRawBundle(t *testing.T, entries []rawEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		typeflag := e.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		hdr := &tar.Header{Name: e.name, Mode: 0o600, Size: int64(len(e.data)), Typeflag: typeflag}
		if typeflag == tar.TypeSymlink {
			hdr.Linkname = "/etc/passwd"
			hdr.Size = 0
		}
		assert.NoError(t, tw.WriteHeader(hdr))
		if typeflag == tar.TypeReg {
			_, err := tw.Write(e.data)
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, tw.Close())
	return buf.Bytes()
}

type rawEntry struct {
	name     string
	data     []byte
	typeflag byte
}

func manifestFor(t *testing.T, payloads map[uint32][]byte) []byte {
	t.Helper()
	m := Manifest{FormatVersion: ManifestFormatVersion, PartitionCount: uint32(len(payloads)), Partitions: map[uint32]PartitionMeta{}}
	for id, p := range payloads {
		m.Partitions[id] = PartitionMeta{SizeBytes: int64(len(p)), SHA256: shaHex(p), SchemaVersion: "0007_x.up.sql"}
	}
	b, err := json.Marshal(m)
	assert.NoError(t, err)
	return b
}

func spoolFilesLeft(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	assert.NoError(t, err)
	return len(entries)
}

func TestOpenBundleRejectsMalformedEntries(t *testing.T) {
	one := gzipBytes(t, sqliteish(t, "one"))
	two := gzipBytes(t, sqliteish(t, "two"))
	payloads := map[uint32][]byte{1: one, 2: two}
	manifest := manifestFor(t, payloads)

	cases := []struct {
		name    string
		entries []rawEntry
		wantErr string
	}{
		{
			name:    "partition id outside the cluster",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "partition-7.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "outside the cluster's partitions",
		},
		{
			name:    "duplicate partition entry",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "partition-1.db.gz", data: one}, {name: "partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "more than once",
		},
		{
			name:    "duplicate manifest",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}, {name: ManifestFileName, data: manifest}},
			wantErr: "more than one manifest.json",
		},
		{
			name:    "directory entry",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "backups/", typeflag: tar.TypeDir}, {name: "partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "not a regular file",
		},
		{
			name:    "symlink entry",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "partition-2.db.gz", typeflag: tar.TypeSymlink}, {name: ManifestFileName, data: manifest}},
			wantErr: "not a regular file",
		},
		{
			name:    "trailing characters in partition name",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "partition-2.db.gz.bak", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "unexpected bundle entry",
		},
		{
			name:    "partition name with directory component",
			entries: []rawEntry{{name: "partition-1.db.gz", data: one}, {name: "x/partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "unexpected bundle entry",
		},
		{
			name:    "partition zero",
			entries: []rawEntry{{name: "partition-0.db.gz", data: one}, {name: "partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "unexpected bundle entry",
		},
		{
			name:    "leading zero in partition id",
			entries: []rawEntry{{name: "partition-01.db.gz", data: one}, {name: "partition-2.db.gz", data: two}, {name: ManifestFileName, data: manifest}},
			wantErr: "unexpected bundle entry",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spool := t.TempDir()
			b, err := OpenBundle(context.Background(), bytes.NewReader(writeRawBundle(t, tc.entries)), spool, 2, RestoreLimits{})
			assert.Nil(t, b)
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Equal(t, 0, spoolFilesLeft(t, spool), "spool files of a rejected bundle must be removed")
		})
	}
}

func TestOpenBundleAcceptsWellFormedRawBundle(t *testing.T) {
	one := gzipBytes(t, sqliteish(t, "one"))
	two := gzipBytes(t, sqliteish(t, "two"))
	payloads := map[uint32][]byte{1: one, 2: two}
	spool := t.TempDir()
	b, err := OpenBundle(context.Background(), bytes.NewReader(writeRawBundle(t, []rawEntry{
		{name: "partition-2.db.gz", data: two},
		{name: "partition-1.db.gz", data: one},
		{name: ManifestFileName, data: manifestFor(t, payloads)},
	})), spool, 2, RestoreLimits{})
	assert.NoError(t, err)
	assert.Equal(t, 2, spoolFilesLeft(t, spool))
	assert.NoError(t, b.Close())
	assert.Equal(t, 0, spoolFilesLeft(t, spool))
}

func TestParsePartitionFileName(t *testing.T) {
	id, ok := ParsePartitionFileName(PartitionFileName(12))
	assert.True(t, ok)
	assert.Equal(t, uint32(12), id)
	for _, bad := range []string{"partition-0.db.gz", "partition-01.db.gz", "partition--1.db.gz", "partition-1.db.gz.bak", "partition-1.db", "sub/partition-1.db.gz", "partition-99999999999.db.gz", "manifest.json"} {
		_, ok := ParsePartitionFileName(bad)
		assert.False(t, ok, bad)
	}
}

func TestDecodeManifestRejectsDuplicateMembersAndTrailingData(t *testing.T) {
	one := gzipBytes(t, sqliteish(t, "one"))
	meta := fmt.Sprintf(`{"snapshotAtMillis":1,"sizeBytes":%d,"sha256":%q,"schemaVersion":"0007_x.up.sql"}`, len(one), shaHex(one))
	cases := map[string]string{
		"duplicate partition key":       `{"formatVersion":1,"partitionCount":1,"partitions":{"1":{},"1":` + meta + `}}`,
		"numerically equal partition":   `{"formatVersion":1,"partitionCount":1,"partitions":{"01":{},"1":` + meta + `}}`,
		"duplicate top-level member":    `{"formatVersion":1,"formatVersion":1,"partitionCount":1,"partitions":{"1":` + meta + `}}`,
		"duplicate nested member":       `{"formatVersion":1,"partitionCount":1,"partitions":{"1":{"sha256":"a","sha256":"b"}}}`,
		"trailing document":             `{"formatVersion":1,"partitionCount":1,"partitions":{"1":` + meta + `}} {}`,
		"duplicate member inside array": `{"formatVersion":1,"partitionCount":1,"partitions":{"1":` + meta + `},"x":[{"a":1,"a":2}]}`,
	}
	for name, manifest := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeManifest(strings.NewReader(manifest), 0)
			require.Error(t, err)
			assert.NotErrorIs(t, err, zenerr.ErrResourceLimit)

			spool := t.TempDir()
			b, err := OpenBundle(context.Background(), bytes.NewReader(writeRawBundle(t, []rawEntry{
				{name: "partition-1.db.gz", data: one},
				{name: ManifestFileName, data: []byte(manifest)},
			})), spool, 1, RestoreLimits{})
			assert.Nil(t, b)
			assert.Error(t, err)
			assert.Equal(t, 0, spoolFilesLeft(t, spool), "spool files of a rejected bundle must be removed")
		})
	}

	valid := `{"formatVersion":1,"partitionCount":1,"partitions":{"1":` + meta + `}}`
	m, err := DecodeManifest(strings.NewReader(valid), 0)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), m.PartitionCount)
	assert.Contains(t, m.Partitions, uint32(1))

	_, err = DecodeManifest(strings.NewReader(valid), 16)
	assert.ErrorIs(t, err, zenerr.ErrResourceLimit)
}

func TestOpenBundleEnforcesSizeLimits(t *testing.T) {
	one := gzipBytes(t, sqliteish(t, "one"))
	payloads := map[uint32][]byte{1: one}
	bundle := writeRawBundle(t, []rawEntry{
		{name: "partition-1.db.gz", data: one},
		{name: ManifestFileName, data: manifestFor(t, payloads)},
	})

	t.Run("stored image too large", func(t *testing.T) {
		spool := t.TempDir()
		_, err := OpenBundle(context.Background(), bytes.NewReader(bundle), spool, 1, RestoreLimits{PartitionImageBytes: int64(len(one)) - 1})
		require.ErrorIs(t, err, zenerr.ErrResourceLimit)
		assert.Equal(t, 0, spoolFilesLeft(t, spool))
	})
	t.Run("decompressed database too large", func(t *testing.T) {
		spool := t.TempDir()
		_, err := OpenBundle(context.Background(), bytes.NewReader(bundle), spool, 1, RestoreLimits{PartitionDatabaseBytes: 17})
		require.ErrorIs(t, err, zenerr.ErrResourceLimit)
		assert.Equal(t, 0, spoolFilesLeft(t, spool))
	})
	t.Run("within limits", func(t *testing.T) {
		b, err := OpenBundle(context.Background(), bytes.NewReader(bundle), t.TempDir(), 1, RestoreLimits{PartitionImageBytes: int64(len(one)), PartitionDatabaseBytes: 32})
		require.NoError(t, err)
		require.NoError(t, b.Close())
	})
}
