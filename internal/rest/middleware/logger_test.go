package middleware

import (
	"bytes"
	"compress/gzip"
	"log/slog"
	"testing"
	"time"
)

func TestDecodeBodyNormalizesContentEncoding(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("decoded body")); err != nil {
		t.Fatalf("write gzip body: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip writer: %v", err)
	}

	decoded, ok := decodeBody(compressed.String(), " GZip ", 1024)
	if !ok {
		t.Fatal("expected gzip body to be decoded")
	}
	if decoded != "decoded body" {
		t.Fatalf("decoded body = %q, want %q", decoded, "decoded body")
	}
}

func TestHeaderFromRecordMatchesCaseInsensitively(t *testing.T) {
	record := slog.NewRecord(time.Time{}, slog.LevelInfo, "", 0)
	record.AddAttrs(slog.Group(
		"headers",
		slog.String("content-encoding", "gzip"),
	))

	if got := headerFromRecord(record, "headers", "Content-Encoding"); got != "gzip" {
		t.Fatalf("header value = %q, want %q", got, "gzip")
	}
}
